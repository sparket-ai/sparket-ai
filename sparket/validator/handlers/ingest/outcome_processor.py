from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import bittensor as bt
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params
from sparket.validator.scoring.validation import validate_outcome_result, ValidationError


_SELECT_PENDING_INBOX = text(
    """
    SELECT id, payload, created_at, retry_count
    FROM inbox
    WHERE topic = 'outcome.submit'
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

_SELECT_MARKETS_FOR_EVENT = text(
    """
    SELECT
        m.market_id,
        m.kind,
        m.line,
        m.points_team_id,
        e.home_team_id,
        e.away_team_id
    FROM market m
    JOIN event e ON m.event_id = e.event_id
    WHERE m.event_id = :event_id
    """
)

_UPSERT_OUTCOME = text(
    """
    INSERT INTO outcome (
        market_id, settled_at, result, score_home, score_away, details
    ) VALUES (
        :market_id, :settled_at, :result, :score_home, :score_away, :details
    )
    ON CONFLICT (market_id) DO UPDATE SET
        settled_at = EXCLUDED.settled_at,
        result = EXCLUDED.result,
        score_home = EXCLUDED.score_home,
        score_away = EXCLUDED.score_away,
        details = EXCLUDED.details
    """
)


async def run_outcome_processing_if_due(
    *,
    validator: Any,
    database: Any,
    limit: int = 100,
) -> int:
    core_cfg = getattr(validator, "app_config", None)
    timers = getattr(getattr(core_cfg, "core", None), "timers", None)
    steps_interval = 10
    if timers is not None:
        try:
            steps_interval = int(getattr(timers, "outcome_process_steps", steps_interval))
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

    processed = 0
    for row in rows:
        inbox_id = row["id"]
        payload = row["payload"]
        created_at = row.get("created_at") or datetime.now(timezone.utc)

        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
            if not isinstance(payload, dict):
                raise ValueError("payload_not_dict")

            raw = payload.get("raw")
            if isinstance(raw, dict):
                # New envelope shape: payload contains raw miner body.
                event_id = raw.get("event_id")
                outcome = {
                    "result": raw.get("result"),
                    "score_home": raw.get("score_home"),
                    "score_away": raw.get("score_away"),
                    "details": raw.get("details"),
                }
            else:
                # Legacy envelope shape.
                event_id = payload.get("event_id")
                outcome = payload.get("outcome") or {}

            if event_id is None:
                raise ValueError("missing_event_id")
            if not isinstance(outcome, dict):
                raise ValueError("outcome_not_dict")

            result = _normalize_result(outcome.get("result"))
            score_home = _parse_int(outcome.get("score_home"))
            score_away = _parse_int(outcome.get("score_away"))

            markets = await database.read(
                _SELECT_MARKETS_FOR_EVENT,
                params={"event_id": int(event_id)},
                mappings=True,
            )

            for market in markets:
                market_result = _resolve_market_result(
                    market_kind=str(market["kind"]),
                    result=result,
                    score_home=score_home,
                    score_away=score_away,
                    line=market.get("line"),
                    points_team_id=market.get("points_team_id"),
                    home_team_id=market.get("home_team_id"),
                    away_team_id=market.get("away_team_id"),
                )
                if market_result is None:
                    continue

                details = json.dumps({
                    "source": "miner",
                    "miner_hotkey": payload.get("miner_hotkey"),
                    "received_at": payload.get("received_at") or created_at.isoformat(),
                    "raw": raw if isinstance(raw, dict) else payload.get("raw"),
                })

                await database.write(
                    _UPSERT_OUTCOME,
                    params={
                        "market_id": market["market_id"],
                        "settled_at": created_at,
                        "result": market_result.upper(),
                        "score_home": score_home,
                        "score_away": score_away,
                        "details": details,
                    },
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
        bt.logging.info({"outcome_inbox_processed": processed})

    return processed


def _normalize_result(value: Any) -> Optional[str]:
    try:
        return validate_outcome_result(value)
    except ValidationError:
        return None


def _parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _resolve_market_result(
    *,
    market_kind: str,
    result: Optional[str],
    score_home: Optional[int],
    score_away: Optional[int],
    line: Any,
    points_team_id: Any,
    home_team_id: Any,
    away_team_id: Any,
) -> Optional[str]:
    kind = market_kind.lower()

    if kind in ("moneyline", "draw_no_bet"):
        if result in ("home", "away", "draw", "void", "push"):
            if kind == "draw_no_bet" and result == "draw":
                return "push"
            return result
        return None

    if kind == "total":
        if result in ("over", "under", "push"):
            return result
        if score_home is None or score_away is None or line is None:
            return None
        total = score_home + score_away
        try:
            line_val = float(line)
        except (TypeError, ValueError):
            return None
        if total > line_val:
            return "over"
        if total < line_val:
            return "under"
        return "push"

    if kind == "spread":
        if score_home is None or score_away is None or line is None:
            return None
        try:
            line_val = float(line)
        except (TypeError, ValueError):
            return None
        if points_team_id is None or home_team_id is None or away_team_id is None:
            return None
        if int(points_team_id) == int(home_team_id):
            adj_home = score_home + line_val
            adj_away = score_away
        elif int(points_team_id) == int(away_team_id):
            adj_home = score_home
            adj_away = score_away + line_val
        else:
            return None
        if adj_home > adj_away:
            return "home"
        if adj_home < adj_away:
            return "away"
        return "push"

    return None


__all__ = ["run_outcome_processing_if_due"]
