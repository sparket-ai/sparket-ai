# handler class for ingesting outcomes from miner submissions -> validator database

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Set

import bittensor as bt

from sqlalchemy import text

from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.events.miner_events import MinerOutcomePushed
from sparket.validator.scoring.validation import validate_outcome_result
from sparket.validator.config.scoring_params import get_scoring_params


# Configuration


_INSERT_INBOX = text(
    """
    INSERT INTO inbox (
        topic, payload, created_at, processed, retry_count, dedupe_key
    ) VALUES (
        :topic, :payload, :created_at, false, 0, :dedupe_key
    )
    """
)

# Check if we already have this submission (deduplication)
_CHECK_DEDUPE = text(
    """
    SELECT 1 FROM inbox WHERE dedupe_key = :dedupe_key LIMIT 1
    """
)

_COUNT_EVENT_OUTCOMES = text(
    """
    SELECT COUNT(1) AS total
    FROM inbox
    WHERE topic = 'outcome.submit'
      AND payload::jsonb ->> 'event_id' = :event_id
      AND payload::jsonb ->> 'miner_hotkey' = :miner_hotkey
      AND created_at >= :since
    """
)

# Query to validate event exists and is eligible for outcome submission
_SELECT_VALID_EVENT = text(
    """
    SELECT e.event_id, e.status, e.start_time_utc
    FROM event e
    WHERE e.event_id = :event_id
      AND e.status IN ('scheduled', 'in_play', 'finished')
    """
)


def _bucket_key(event_id: int, miner_hotkey: str, received_at: datetime, bucket_seconds: int) -> str:
    """Deterministic dedupe key for outcome envelopes (5 min buckets)."""
    epoch = int(received_at.replace(tzinfo=timezone.utc).timestamp())
    bucket = epoch - (epoch % bucket_seconds)
    return f"outcome:{event_id}:{miner_hotkey}:{bucket}"


class IngestOutcomeHandler:
    """Handles miner outcome submissions (Task 3 - Outcome Verification).
    
    Miners report game results which are later cross-referenced with official data.
    
    Validation:
    - Event must exist and be in valid state (scheduled/in_play/finished)
    - Game must have started (can't report outcome for future games)
    - Outcome window: up to 48 hours after game start
    - Basic payload validation (has required fields)
    
    Outcomes are queued in inbox for async processing.
    """
    
    def __init__(self, database: Any):
        self.database = database
        self._ingest_params = get_scoring_params().ingest

    async def handle_synapse(self, synapse: SparketSynapse) -> MinerOutcomePushed | None:
        """Queue OUTCOME_PUSH for asynchronous processing and return immediately."""
        if synapse.type != SparketSynapseType.OUTCOME_PUSH:
            return None

        miner_hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None) or ""
        raw = synapse.payload if isinstance(synapse.payload, dict) else {}
        received_at = datetime.now(timezone.utc)

        event_id = self._extract_event_id(raw)
        dedupe_event_id = event_id if event_id is not None else 0
        envelope = {
            "topic": "outcome.submit",
            "payload": json.dumps({
                "miner_hotkey": miner_hotkey,
                "received_at": received_at.isoformat(),
                "raw": raw,
            }),
            "created_at": received_at,
            "dedupe_key": _bucket_key(
                dedupe_event_id,
                miner_hotkey,
                received_at,
                self._ingest_params.outcome_bucket_seconds,
            ),
        }

        accepted = False
        try:
            await self.database.write(_INSERT_INBOX, params=envelope)
            accepted = True
        except Exception as e:
            bt.logging.warning({"ingest_outcome_enqueue_error": str(e)})

        return MinerOutcomePushed(
            miner_hotkey=miner_hotkey,
            payload={
                "accepted": accepted,
                "queued": accepted,
                "event_id": event_id,
            },
        )

    async def _within_daily_event_cap(
        self,
        event_id: int,
        miner_hotkey: str,
        now: datetime,
    ) -> bool:
        since = now - timedelta(days=1)
        rows = await self.database.read(
            _COUNT_EVENT_OUTCOMES,
            params={
                "event_id": str(event_id),
                "miner_hotkey": miner_hotkey,
                "since": since,
            },
            mappings=True,
        )
        total = int(rows[0]["total"]) if rows else 0
        return total < self._ingest_params.max_outcomes_per_event_day
    
    def _extract_event_id(self, payload: dict) -> Optional[int]:
        """Extract event_id from payload."""
        event_id = payload.get("event_id")
        if event_id is None:
            return None
        try:
            return int(event_id)
        except (ValueError, TypeError):
            return None
    
    def _extract_outcome_data(self, payload: dict) -> Optional[Dict[str, Any]]:
        """Extract and validate outcome data from payload.
        
        Expected structure:
        {
            "event_id": 123,
            "result": "HOME" | "AWAY" | "DRAW",
            "score_home": 2,
            "score_away": 1,
            "details": {...}  # optional
        }
        """
        result = payload.get("result")
        score_home = payload.get("score_home")
        score_away = payload.get("score_away")
        
        # Must have either result or scores
        if result is None and (score_home is None or score_away is None):
            return None
        
        outcome = {}
        
        if result is not None:
            # Validate result is a valid value
            try:
                outcome["result"] = validate_outcome_result(result)
            except Exception:
                return None
        
        if score_home is not None:
            try:
                outcome["score_home"] = int(score_home)
            except (ValueError, TypeError):
                return None
        
        if score_away is not None:
            try:
                outcome["score_away"] = int(score_away)
            except (ValueError, TypeError):
                return None
        
        if payload.get("details"):
            outcome["details"] = payload["details"]
        
        return outcome
    
    async def _validate_event(self, event_id: int, now: datetime) -> Dict[str, Any]:
        """Validate event is eligible for outcome submission.
        
        Rules:
        - Event must exist with valid status (scheduled/in_play/finished)
        - Game must have STARTED (can't report outcome before game begins)
        - Outcome window: from game start to 12 hours after start
        """
        rows = await self.database.read(
            _SELECT_VALID_EVENT,
            params={"event_id": event_id},
            mappings=True,
        )
        
        if not rows:
            return {"valid": False, "reason": "event_not_found_or_ineligible"}
        
        row = rows[0]
        status = row["status"]
        start_time = row["start_time_utc"]
        
        # Ensure start_time is timezone-aware
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        
        # Game must have started - can't report outcome before it begins!
        if now < start_time:
            return {"valid": False, "reason": "game_not_started", "status": status}
        
        # Outcome window: up to 12 hours after game start
        cutoff = start_time + timedelta(hours=self._ingest_params.outcome_window_hours)
        if now > cutoff:
            return {"valid": False, "reason": "outcome_window_closed", "status": status}
        
        return {"valid": True, "status": status, "start_time": start_time}
