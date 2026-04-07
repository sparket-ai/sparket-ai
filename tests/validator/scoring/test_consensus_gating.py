from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from sparket.validator.handlers.score.odds_score import OddsScoreHandler
from sparket.validator.handlers.score.outcome_score import OutcomeScoreHandler


@pytest.mark.asyncio
async def test_odds_score_skips_low_consensus():
    db = MagicMock()
    db.read = AsyncMock(return_value=[])
    db.write = AsyncMock()
    handler = OddsScoreHandler(db)
    handler._min_books_for_consensus = 3

    async def fake_gt(*_args, **_kwargs):
        return {
            "prob_consensus": 0.5,
            "odds_consensus": 2.0,
            "contributing_books": 2,
            "bias_version": 1,
            "computed_at": datetime.now(timezone.utc),
            "start_time_utc": datetime.now(timezone.utc),
        }

    handler._get_ground_truth = fake_gt  # type: ignore[assignment]

    row = {
        "submission_id": 1,
        "market_id": 10,
        "side": "home",
        "odds_eu": 2.0,
        "imp_prob": 0.5,
        "submitted_at": datetime.now(timezone.utc),
    }
    success, reason = await handler._score_submission_row(row)
    assert success is False
    assert reason == "insufficient_books"
    db.write.assert_not_called()


@pytest.mark.asyncio
async def test_outcome_score_skips_low_consensus():
    db = MagicMock()
    db.read = AsyncMock(return_value=[])
    db.write = AsyncMock()
    handler = OutcomeScoreHandler(db)
    handler._min_books_for_consensus = 3

    async def fake_gt(*_args, **_kwargs):
        return {"home": 0.6, "away": 0.4}, 2

    handler._get_ground_truth_probs = fake_gt  # type: ignore[assignment]

    market_row = {
        "market_id": 1,
        "result": "home",
        "settled_at": datetime.now(timezone.utc),
        "kind": "spread",
    }
    scored, reason = await handler._score_market_submissions(market_row)
    assert scored == 0
    assert reason == "insufficient_books"
    db.write.assert_not_called()
