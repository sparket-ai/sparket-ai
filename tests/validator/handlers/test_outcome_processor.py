import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from sparket.validator.handlers.ingest.outcome_processor import (
    _resolve_market_result,
    run_outcome_processing_if_due,
)


@pytest.mark.asyncio
async def test_outcome_processor_upserts_moneyline():
    now = datetime.now(timezone.utc)
    payload = {
        "event_id": 10,
        "miner_hotkey": "hk",
        "received_at": now.isoformat(),
        "outcome": {"result": "HOME", "score_home": 3, "score_away": 2},
    }
    inbox_rows = [{"id": 1, "payload": json.dumps(payload), "created_at": now}]
    market_rows = [
        {"market_id": 100, "kind": "moneyline", "line": None, "points_team_id": None, "home_team_id": 1, "away_team_id": 2},
    ]

    db = MagicMock()
    db.read = AsyncMock(side_effect=[inbox_rows, market_rows])
    db.write = AsyncMock(return_value=1)

    validator = MagicMock()
    validator.step = 0
    validator.app_config.core.timers.outcome_process_steps = 1

    processed = await run_outcome_processing_if_due(validator=validator, database=db)

    assert processed == 1
    params_list = [
        call.kwargs.get("params") or call[1].get("params")
        for call in db.write.call_args_list
    ]
    assert any(p.get("result") == "HOME" for p in params_list if isinstance(p, dict))


def test_resolve_market_result_total():
    result = _resolve_market_result(
        market_kind="total",
        result=None,
        score_home=2,
        score_away=1,
        line=2.5,
        points_team_id=None,
        home_team_id=None,
        away_team_id=None,
    )
    assert result == "over"
