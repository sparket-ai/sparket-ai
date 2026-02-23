# handler class for ingesting odds from miner submissions -> validator database

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import bittensor as bt

from sqlalchemy import text

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.events.miner_events import MinerOddsPushed
from sparket.validator.config.scoring_params import get_scoring_params
from sparket.validator.scoring.validation import get_validator
from sparket.validator.scoring.types import ValidationError
from sparket.shared.enums import PriceSide
from sparket.shared.rows import MinerSubmissionRow

_VALID_SIDES = {s.value for s in PriceSide}


# Configuration
IMP_PROB_EPSILON = 1e-4   # Max tolerated odds/prob mismatch


def _floor_to_bucket(dt: datetime, bucket_seconds: int) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    epoch = int(dt.timestamp())
    bucket = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(bucket, tz=timezone.utc)


def _compute_imp_prob(odds_eu: float, imp_prob: float | None) -> Optional[float]:
    validator = get_validator()
    try:
        odds_dec = validator.validate_odds(odds_eu)
        computed = validator.odds_to_prob(odds_dec)
    except ValidationError:
        return None

    if imp_prob is not None:
        try:
            provided = validator.validate_probability(imp_prob)
            delta = abs(float(computed) - float(provided))
            if delta > IMP_PROB_EPSILON:
                bt.logging.debug({"ingest_odds_prob_mismatch": {"delta": delta}})
        except ValidationError:
            bt.logging.debug({"ingest_odds_prob_invalid": True})

    return float(computed)


def _validate_priced_at(
    priced_at_raw: Any,
    received_at: datetime,
    tolerance_sec: int,
) -> Optional[datetime]:
    """Validate and bound priced_at timestamp.
    
    Returns None if invalid or outside tolerance window.
    """
    if priced_at_raw is None:
        return None
    
    try:
        if isinstance(priced_at_raw, datetime):
            priced_at = priced_at_raw
        elif isinstance(priced_at_raw, str):
            priced_at = datetime.fromisoformat(priced_at_raw.replace("Z", "+00:00"))
        else:
            return None
        
        # Ensure timezone-aware
        if priced_at.tzinfo is None:
            priced_at = priced_at.replace(tzinfo=timezone.utc)
        
        # Bound to ±5 minutes of received_at (prevents future/stale claims)
        delta = abs((priced_at - received_at).total_seconds())
        if delta > tolerance_sec:
            bt.logging.debug({"priced_at_rejected": "outside_tolerance", "delta_sec": delta})
            return None
        
        return priced_at
    except Exception:
        return None


def _coerce_submit_odds_to_rows(
    payload: dict, 
    miner_id: int,  # Now derived from hotkey lookup, not payload
    miner_hotkey: str, 
    received_at: datetime,
    valid_market_ids: Set[int],
    *,
    bucket_seconds: int,
    priced_at_tolerance_sec: int,
) -> List[MinerSubmissionRow]:
    """Parse submissions into rows, filtering to valid markets only.
    
    Security:
    - miner_id is derived from authenticated hotkey, not trusted from payload
    - odds_eu bounded to (1.01, 1000) to prevent outlier gaming
    - priced_at bounded to ±5 minutes of received_at
    """
    rows: List[MinerSubmissionRow] = []
    submitted_at = _floor_to_bucket(received_at, bucket_seconds)
    submissions = payload.get("submissions", []) or []
    
    for sub in submissions:
        market_id = int(sub.get("market_id", 0))
        
        # Skip invalid markets
        if market_id not in valid_market_ids:
            bt.logging.debug({"ingest_odds_skip": "invalid_market", "market_id": market_id})
            continue
        
        kind = sub.get("kind")
        priced_at = _validate_priced_at(
            sub.get("priced_at"),
            received_at,
            priced_at_tolerance_sec,
        )
        prices = sub.get("prices", []) or []
        
        for price in prices:
            side_raw = price.get("side")
            if not isinstance(side_raw, str) or side_raw.lower() not in _VALID_SIDES:
                bt.logging.debug({"ingest_odds_skip": "invalid_side", "side": side_raw})
                continue
            side = side_raw.upper()
            try:
                odds_eu = float(price.get("odds_eu", 0))
                imp_prob = _compute_imp_prob(odds_eu, price.get("imp_prob"))
                if imp_prob is None:
                    continue
            except (ValueError, TypeError, ZeroDivisionError):
                continue
            
            rows.append({
                "miner_id": miner_id,
                "miner_hotkey": miner_hotkey,
                "market_id": market_id,
                "side": side,
                "submitted_at": submitted_at,
                "priced_at": priced_at,
                "odds_eu": odds_eu,
                "imp_prob": imp_prob,
                "payload": json.dumps({"kind": kind} if kind is not None else {}),
            })
    
    return rows


# Query to get valid market IDs (markets for events starting within window)
_SELECT_VALID_MARKETS = text(
    """
    SELECT m.market_id
    FROM market m
    JOIN event e ON m.event_id = e.event_id
    WHERE m.market_id = ANY(:market_ids)
      AND e.status = 'scheduled'
      AND e.start_time_utc >= :now
      AND e.start_time_utc <= :window_end
    """
)

# Query to get miner_id from authenticated hotkey (SECURITY: never trust miner_id from payload)
_SELECT_MINER_BY_HOTKEY = text(
    """
    SELECT miner_id FROM miner WHERE hotkey = :hotkey LIMIT 1
    """
)

_INSERT_MINER_SUBMISSION = text(
    """
    INSERT INTO miner_submission (
        miner_id, miner_hotkey, market_id, side, submitted_at, priced_at, odds_eu, imp_prob, payload
    ) VALUES (
        :miner_id, :miner_hotkey, :market_id, :side, :submitted_at, :priced_at, :odds_eu, :imp_prob, :payload
    ) ON CONFLICT DO NOTHING
    """
)

_COUNT_MARKET_SUBMISSIONS = text(
    """
    SELECT COUNT(1) AS total
    FROM miner_submission
    WHERE miner_id = :miner_id
      AND miner_hotkey = :miner_hotkey
      AND market_id = :market_id
      AND submitted_at >= :since
    """
)

# Batch query: count submissions per market for multiple markets at once
_COUNT_MARKET_SUBMISSIONS_BATCH = text(
    """
    SELECT market_id, COUNT(1) AS total
    FROM miner_submission
    WHERE miner_id = :miner_id
      AND miner_hotkey = :miner_hotkey
      AND market_id = ANY(:market_ids)
      AND submitted_at >= :since
    GROUP BY market_id
    """
)

_INSERT_INBOX = text(
    """
    INSERT INTO inbox (
        topic, payload, created_at, processed, retry_count, dedupe_key
    ) VALUES (
        :topic, :payload, :created_at, false, 0, :dedupe_key
    )
    """
)

_SELECT_PENDING_INBOX = text(
    """
    SELECT id, payload, created_at, retry_count
    FROM inbox
    WHERE topic = 'odds.submit'
      AND processed = false
      AND retry_count < :max_retries
    ORDER BY created_at ASC
    LIMIT :limit
    """
)

_MARK_INBOX_PROCESSED = text(
    """
    UPDATE inbox
    SET processed = true,
        processed_at = :processed_at
    WHERE id = :id
    """
)

_MARK_INBOX_FAILED = text(
    """
    UPDATE inbox
    SET retry_count = retry_count + 1,
        last_error = :error,
        processed = CASE
            WHEN retry_count + 1 >= :max_retries THEN true
            ELSE processed
        END,
        processed_at = CASE
            WHEN retry_count + 1 >= :max_retries THEN :processed_at
            ELSE processed_at
        END
    WHERE id = :id
    """
)


def _odds_dedupe_key(miner_hotkey: str, received_at: datetime, bucket_seconds: int) -> str:
    epoch = int(received_at.replace(tzinfo=timezone.utc).timestamp())
    bucket = epoch - (epoch % bucket_seconds)
    return f"odds:{miner_hotkey}:{bucket}"


class IngestOddsHandler:
    """Handles miner odds submissions.
    
    Security:
    - miner_id derived from authenticated hotkey (never trusted from payload)
    - odds_eu bounded to (1.01, 1000) to prevent outlier gaming
    - priced_at bounded to ±5 minutes of received_at
    - Only accepts odds for markets on events starting within 7 days
    """
    
    def __init__(self, database: Any):
        self.database = database
        self._ingest_params = get_scoring_params().ingest

    async def handle_synapse(self, synapse: SparketSynapse) -> MinerOddsPushed | None:
        """Queue ODDS_PUSH for asynchronous processing and return immediately."""
        if synapse.type != SparketSynapseType.ODDS_PUSH:
            return None

        miner_hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None) or ""
        raw = synapse.payload if isinstance(synapse.payload, dict) else {}
        received_at = datetime.now(timezone.utc)

        envelope = {
            "topic": "odds.submit",
            "payload": json.dumps({
                "miner_hotkey": miner_hotkey,
                "received_at": received_at.isoformat(),
                "raw": raw,
            }),
            "created_at": received_at,
            "dedupe_key": _odds_dedupe_key(
                miner_hotkey,
                received_at,
                self._ingest_params.odds_bucket_seconds,
            ),
        }
        accepted = False
        try:
            await self.database.write(_INSERT_INBOX, params=envelope)
            accepted = True
        except Exception as e:
            bt.logging.warning({"ingest_odds_enqueue_error": str(e)})

        return MinerOddsPushed(
            miner_hotkey=miner_hotkey,
            payload={
                "accepted": accepted,
                "queued": accepted,
            },
        )

    async def process_enqueued_payload(
        self,
        *,
        miner_hotkey: str,
        raw: dict,
        received_at: datetime,
    ) -> Dict[str, int]:
        """Process queued odds payload outside request/response flow."""
        # SECURITY: Derive miner_id from authenticated hotkey, NEVER trust payload
        miner_id = await self._get_miner_id_from_hotkey(miner_hotkey)
        if miner_id is None:
            bt.logging.warning({"ingest_odds_rejected": "miner_not_registered", "hotkey": miner_hotkey[:16] + "..."})
            return {"rows": 0, "persisted": 0, "rejected": 0}

        # Extract market IDs from submission
        requested_market_ids = self._extract_market_ids(raw)
        if not requested_market_ids:
            bt.logging.debug({"ingest_odds": "no_markets_in_submission"})
            return {"rows": 0, "persisted": 0, "rejected": 0}

        # Validate which markets are within submission window
        valid_market_ids = await self._get_valid_market_ids(requested_market_ids, received_at)
        rejected_count = len(requested_market_ids) - len(valid_market_ids)

        # Parse and filter submissions (miner_id derived from hotkey, not payload)
        rows = await self._coerce_with_caps(
            payload=raw,
            miner_id=miner_id,
            miner_hotkey=miner_hotkey,
            received_at=received_at,
            valid_market_ids=valid_market_ids,
        )

        # Persist valid rows in a single transaction
        persisted = 0
        if rows:
            try:
                persisted = await self.database.write_many(_INSERT_MINER_SUBMISSION, rows)
            except Exception as e:
                bt.logging.warning({"ingest_odds_persist_error": str(e)})

        return {"rows": len(rows), "persisted": persisted, "rejected": rejected_count}

    async def _coerce_with_caps(
        self,
        *,
        payload: dict,
        miner_id: int,
        miner_hotkey: str,
        received_at: datetime,
        valid_market_ids: Set[int],
    ) -> List[MinerSubmissionRow]:
        rows: List[MinerSubmissionRow] = []
        submissions = payload.get("submissions", []) or []
        
        # Extract all market IDs from submissions that are valid
        submission_market_ids = set()
        for sub in submissions:
            market_id = int(sub.get("market_id", 0))
            if market_id in valid_market_ids:
                submission_market_ids.add(market_id)
        
        if not submission_market_ids:
            return rows
        
        # Batch check: get submission counts for all markets in ONE query
        markets_within_cap = await self._get_markets_within_cap(
            miner_id=miner_id,
            miner_hotkey=miner_hotkey,
            market_ids=submission_market_ids,
            now=received_at,
        )
        
        # Now process submissions, filtering by cap results
        for sub in submissions:
            market_id = int(sub.get("market_id", 0))
            if market_id not in valid_market_ids:
                continue
            if market_id not in markets_within_cap:
                bt.logging.info({
                    "ingest_odds_rate_limited": {
                        "miner_id": miner_id,
                        "market_id": market_id,
                        "cap": self._ingest_params.max_submissions_per_market_day,
                    }
                })
                continue
            rows.extend(
                _coerce_submit_odds_to_rows(
                    payload={"submissions": [sub]},
                    miner_id=miner_id,
                    miner_hotkey=miner_hotkey,
                    received_at=received_at,
                    valid_market_ids=valid_market_ids,
                    bucket_seconds=self._ingest_params.odds_bucket_seconds,
                    priced_at_tolerance_sec=self._ingest_params.priced_at_tolerance_sec,
                )
            )
        return rows
    
    async def _get_markets_within_cap(
        self,
        *,
        miner_id: int,
        miner_hotkey: str,
        market_ids: Set[int],
        now: datetime,
    ) -> Set[int]:
        """Batch check which markets are within daily submission cap.
        
        Returns set of market_ids that have room for more submissions.
        Single query instead of N queries for N markets.
        """
        if not market_ids:
            return set()
        
        since = now - timedelta(days=1)
        cap = self._ingest_params.max_submissions_per_market_day
        
        # Get counts for all markets in one query
        rows = await self.database.read(
            _COUNT_MARKET_SUBMISSIONS_BATCH,
            params={
                "miner_id": miner_id,
                "miner_hotkey": miner_hotkey,
                "market_ids": list(market_ids),
                "since": since,
            },
            mappings=True,
        )
        
        # Build set of markets that have hit the cap
        over_cap = set()
        for row in rows:
            if int(row["total"]) >= cap:
                over_cap.add(int(row["market_id"]))
        
        # Return markets that are still within cap
        return market_ids - over_cap

    async def _within_daily_market_cap(
        self,
        *,
        miner_id: int,
        miner_hotkey: str,
        market_id: int,
        now: datetime,
    ) -> bool:
        since = now - timedelta(days=1)
        rows = await self.database.read(
            _COUNT_MARKET_SUBMISSIONS,
            params={
                "miner_id": miner_id,
                "miner_hotkey": miner_hotkey,
                "market_id": market_id,
                "since": since,
            },
            mappings=True,
        )
        total = int(rows[0]["total"]) if rows else 0
        return total < self._ingest_params.max_submissions_per_market_day
    
    async def _get_miner_id_from_hotkey(self, hotkey: str) -> Optional[int]:
        """Securely look up miner_id from authenticated hotkey."""
        if not hotkey:
            return None
        rows = await self.database.read(
            _SELECT_MINER_BY_HOTKEY,
            params={"hotkey": hotkey},
            mappings=True,
        )
        if rows:
            return int(rows[0]["miner_id"])
        return None
    
    def _extract_market_ids(self, payload: dict) -> Set[int]:
        """Extract all market IDs from submission payload."""
        market_ids = set()
        submissions = payload.get("submissions", []) or []
        for sub in submissions:
            market_id = sub.get("market_id")
            if market_id is not None:
                try:
                    market_ids.add(int(market_id))
                except (ValueError, TypeError):
                    pass
        return market_ids
    
    async def _get_valid_market_ids(self, market_ids: Set[int], now: datetime) -> Set[int]:
        """Return market IDs that are valid for submission (within time window)."""
        if not market_ids:
            return set()
        
        window_end = now + timedelta(days=self._ingest_params.odds_window_days)
        
        rows = await self.database.read(
            _SELECT_VALID_MARKETS,
            params={
                "market_ids": list(market_ids),
                "now": now,
                "window_end": window_end,
            },
            mappings=True,
        )
        
        return {int(r["market_id"]) for r in rows}


async def run_odds_processing_if_due(
    *,
    validator: Any,
    database: Any,
    limit: int = 200,
) -> int:
    timers = getattr(getattr(getattr(validator, "app_config", None), "core", None), "timers", None)
    steps_interval = 1
    if timers is not None:
        try:
            steps_interval = int(getattr(timers, "odds_process_steps", 1))
        except Exception:
            pass
    if steps_interval <= 0 or (validator.step % steps_interval != 0):
        return 0

    max_retries = int(get_scoring_params().ingest.outcome_max_retries)
    rows = await database.read(
        _SELECT_PENDING_INBOX,
        params={"limit": limit, "max_retries": max_retries},
        mappings=True,
    )
    if not rows:
        return 0

    handler = IngestOddsHandler(database)
    processed = 0
    for row in rows:
        inbox_id = row["id"]
        payload = row.get("payload")
        created_at = row.get("created_at") or datetime.now(timezone.utc)
        try:
            if not isinstance(payload, dict):
                raise ValueError("odds_payload_not_dict")

            miner_hotkey = str(payload.get("miner_hotkey") or "")
            raw = payload.get("raw")
            if not isinstance(raw, dict):
                raise ValueError("odds_raw_not_dict")

            received_at_raw = payload.get("received_at")
            if isinstance(received_at_raw, str):
                received_at = datetime.fromisoformat(received_at_raw.replace("Z", "+00:00"))
                if received_at.tzinfo is None:
                    received_at = received_at.replace(tzinfo=timezone.utc)
            else:
                received_at = created_at if isinstance(created_at, datetime) else datetime.now(timezone.utc)

            await handler.process_enqueued_payload(
                miner_hotkey=miner_hotkey,
                raw=raw,
                received_at=received_at,
            )
            await database.write(
                _MARK_INBOX_PROCESSED,
                params={"id": inbox_id, "processed_at": datetime.now(timezone.utc)},
            )
            processed += 1
        except Exception as exc:
            await database.write(
                _MARK_INBOX_FAILED,
                params={
                    "id": inbox_id,
                    "error": str(exc)[:1000],
                    "processed_at": datetime.now(timezone.utc),
                    "max_retries": max_retries,
                },
            )

    if processed:
        bt.logging.info({"odds_inbox_processed": processed})

    return processed
