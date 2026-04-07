import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from sparket.validator.handlers.score.outcome_score import OutcomeScoreHandler


def _make_db(read_side_effect):
    db = MagicMock()
    db.read = AsyncMock(side_effect=read_side_effect)
    db.write = AsyncMock()
    return db


@pytest.mark.asyncio
async def test_moneyline_partial_submission_skipped():
    now = datetime.now(timezone.utc)
    market_row = {
        "market_id": 1,
        "result": "home",
        "settled_at": now,
        "kind": "moneyline",
    }
    gt_probs = [
        {"side": "home", "prob_consensus": 0.55, "contributing_books": 3},
        {"side": "away", "prob_consensus": 0.35, "contributing_books": 3},
        {"side": "draw", "prob_consensus": 0.10, "contributing_books": 3},
    ]
    submissions = [
        {
            "submission_id": 10,
            "miner_id": 1,
            "miner_hotkey": "hk",
            "market_id": 1,
            "side": "home",
            "imp_prob": 0.6,
            "submitted_at": now,
        }
    ]
    db = _make_db([gt_probs, submissions])
    handler = OutcomeScoreHandler(db)

    scored, reason = await handler._score_market_submissions(market_row)

    assert scored == 0
    db.write.assert_not_called()


@pytest.mark.asyncio
async def test_two_way_single_side_uses_complement():
    now = datetime.now(timezone.utc)
    market_row = {
        "market_id": 2,
        "result": "home",
        "settled_at": now,
        "kind": "spread",
    }
    gt_probs = [
        {"side": "home", "prob_consensus": 0.52, "contributing_books": 2},
        {"side": "away", "prob_consensus": 0.48, "contributing_books": 2},
    ]
    submissions = [
        {
            "submission_id": 20,
            "miner_id": 2,
            "miner_hotkey": "hk2",
            "market_id": 2,
            "side": "home",
            "imp_prob": 0.7,
            "submitted_at": now,
        }
    ]
    db = _make_db([gt_probs, submissions])
    handler = OutcomeScoreHandler(db)

    scored, reason = await handler._score_market_submissions(market_row)

    assert scored == 1
    db.write.assert_called_once()
    params = db.write.call_args.kwargs["params"]
    assert params["submission_id"] == 20
    assert json.loads(params["outcome_vector"]) == [1, 0]


@pytest.mark.asyncio
async def test_moneyline_full_vector_scores_all_sides():
    now = datetime.now(timezone.utc)
    market_row = {
        "market_id": 3,
        "result": "draw",
        "settled_at": now,
        "kind": "moneyline",
    }
    gt_probs = [
        {"side": "home", "prob_consensus": 0.45, "contributing_books": 3},
        {"side": "away", "prob_consensus": 0.45, "contributing_books": 3},
        {"side": "draw", "prob_consensus": 0.10, "contributing_books": 3},
    ]
    submissions = [
        {
            "submission_id": 30,
            "miner_id": 3,
            "miner_hotkey": "hk3",
            "market_id": 3,
            "side": "home",
            "imp_prob": 0.4,
            "submitted_at": now,
        },
        {
            "submission_id": 31,
            "miner_id": 3,
            "miner_hotkey": "hk3",
            "market_id": 3,
            "side": "away",
            "imp_prob": 0.4,
            "submitted_at": now,
        },
        {
            "submission_id": 32,
            "miner_id": 3,
            "miner_hotkey": "hk3",
            "market_id": 3,
            "side": "draw",
            "imp_prob": 0.2,
            "submitted_at": now,
        },
    ]
    db = _make_db([gt_probs, submissions])
    handler = OutcomeScoreHandler(db)

    scored, reason = await handler._score_market_submissions(market_row)

    assert scored == 3
    assert db.write.call_count == 3
