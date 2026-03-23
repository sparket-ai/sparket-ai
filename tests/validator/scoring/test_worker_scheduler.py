"""Tests for scoring worker scheduler."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from sparket.validator.scoring.worker import scheduler
from sparket.validator.scoring.batch.processor import WorkType


@pytest.mark.asyncio
async def test_creates_rolling_and_skill_work_when_due():
    """Should enqueue rolling and skill work for an empty run."""
    mock_db = MagicMock()
    mock_db.read = AsyncMock(return_value=[])

    mock_queue = MagicMock()
    mock_queue.get_status_counts = AsyncMock(
        side_effect=[
            {},  # rolling initial
            {"completed": 1},  # rolling after create
            {},  # composite_uniqueness initial
            {"completed": 1},  # composite_uniqueness after create
            {},  # shapley initial
            {"completed": 1},  # shapley after create
            {},  # skill initial
        ]
    )
    mock_queue.create_work_batch = AsyncMock()

    with patch("sparket.validator.scoring.worker.scheduler.WorkQueue", return_value=mock_queue):
        await scheduler._schedule_scoring_work(mock_db)

    assert mock_queue.create_work_batch.call_count == 4
    call_types = [call.args[0] for call in mock_queue.create_work_batch.call_args_list]
    assert WorkType.ROLLING in call_types
    assert WorkType.COMPOSITE_UNIQUENESS in call_types
    assert WorkType.SHAPLEY in call_types
    assert WorkType.SKILL in call_types


@pytest.mark.asyncio
async def test_stops_when_rolling_incomplete():
    """Should not schedule downstream work when rolling is pending."""
    mock_db = MagicMock()
    mock_db.read = AsyncMock(return_value=[])

    mock_queue = MagicMock()
    mock_queue.get_status_counts = AsyncMock(
        return_value={"pending": 1}
    )
    mock_queue.create_work_batch = AsyncMock()

    with patch("sparket.validator.scoring.worker.scheduler.WorkQueue", return_value=mock_queue):
        await scheduler._schedule_scoring_work(mock_db)

    mock_queue.create_work_batch.assert_not_called()
