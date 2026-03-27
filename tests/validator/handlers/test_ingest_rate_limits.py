from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from sparket.validator.handlers.ingest.ingest_odds import IngestOddsHandler
from sparket.validator.handlers.ingest.ingest_outcome import IngestOutcomeHandler
from sparket.protocol.protocol import SparketSynapse, SparketSynapseType


@pytest.mark.asyncio
async def test_odds_daily_market_cap_blocks():
    db = MagicMock()
    db.read = AsyncMock(
        side_effect=[
            [{"total": 200}],
        ]
    )
    handler = IngestOddsHandler(db)
    allowed = await handler._within_daily_market_cap(
        miner_id=1,
        miner_hotkey="hk",
        market_id=10,
        now=datetime.now(timezone.utc),
    )
    assert allowed is False


@pytest.mark.asyncio
async def test_outcome_daily_event_cap_blocks():
    db = MagicMock()
    db.read = AsyncMock(return_value=[{"total": 50}])
    handler = IngestOutcomeHandler(db)
    allowed = await handler._within_daily_event_cap(
        event_id=1,
        miner_hotkey="hk",
        now=datetime.now(timezone.utc),
    )
    assert allowed is False


@pytest.mark.asyncio
async def test_outcome_handle_synapse_queues_for_processing():
    """handle_synapse now queues to inbox; rate limiting is deferred to processing."""
    db = MagicMock()
    db.write = AsyncMock(return_value=1)
    handler = IngestOutcomeHandler(db)
    syn = SparketSynapse(
        type=SparketSynapseType.OUTCOME_PUSH,
        payload={"event_id": 1, "result": "HOME"},
    )
    result = await handler.handle_synapse(syn)
    assert result is not None
    payload = result.event_data.get("payload", {})
    assert payload.get("accepted") is True
    assert payload.get("queued") is True
    assert payload.get("event_id") == 1
