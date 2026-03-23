"""Scoring work scheduler for worker queue."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, List, Tuple

import bittensor as bt
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params
from sparket.validator.scoring.batch.processor import WorkType
from sparket.validator.scoring.batch.work_queue import WorkQueue
from sparket.validator.scoring.determinism import get_canonical_window_bounds

MINER_CHUNK_SIZE = 100

_SELECT_MINERS_WITH_OUTCOMES = text(
    """
    SELECT DISTINCT ms.miner_id
    FROM miner_submission ms
    JOIN submission_outcome_score sos ON ms.submission_id = sos.submission_id
    WHERE sos.settled_at >= :window_start
      AND sos.settled_at < :window_end
    ORDER BY ms.miner_id
    """
)

_SELECT_MINERS_WITH_SUBMISSIONS = text(
    """
    SELECT DISTINCT miner_id
    FROM miner_submission
    WHERE submitted_at >= :window_start
      AND submitted_at < :window_end
    ORDER BY miner_id
    """
)


def _chunk_ranges(values: Iterable[int], chunk_size: int) -> List[Tuple[int, int]]:
    items = list(values)
    if not items:
        return []
    chunks = []
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        chunks.append((chunk[0], chunk[-1]))
    return chunks


def _run_id_for(window_end: datetime) -> str:
    if window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=timezone.utc)
    return window_end.strftime("%Y%m%d")


def _is_stage_complete(counts: dict[str, int], allow_empty: bool = False) -> bool:
    if not counts:
        return allow_empty
    pending = counts.get("pending", 0)
    claimed = counts.get("claimed", 0)
    failed = counts.get("failed", 0)
    return (pending + claimed + failed) == 0


async def schedule_scoring_work_if_due_async(
    *,
    step: int,
    app_config: Any,
    database: Any,
) -> None:
    """Step-based scheduler for worker scoring tasks."""
    try:
        steps_interval = 25
        try:
            steps_interval = int(getattr(getattr(app_config, "core", None), "timers", None).main_score_steps)  # type: ignore[attr-defined]
        except Exception:
            pass
        if steps_interval > 0 and (step % steps_interval == 0):
            await _schedule_scoring_work(database)
    except Exception as e:
        bt.logging.warning({"scoring_worker_schedule_error": str(e)})


async def _schedule_scoring_work(database: Any) -> None:
    """Create work items for the current scoring run."""
    params = get_scoring_params()
    queue = WorkQueue(database, worker_id="scheduler")

    rolling_days = params.windows.rolling_window_days
    roll_start, roll_end = get_canonical_window_bounds(rolling_days)
    run_id = _run_id_for(roll_end)
    prefix = f"{run_id}:"
    prefix_like = f"{prefix}%"

    # Stage 1: Rolling aggregates (singleton)
    rolling_counts = await queue.get_status_counts(WorkType.ROLLING, chunk_prefix=prefix_like)
    if not rolling_counts:
        await queue.create_work_batch(
            WorkType.ROLLING,
            chunk_keys=[f"{prefix}all"],
            params={
                "run_id": run_id,
                "window_start": roll_start.isoformat(),
                "window_end": roll_end.isoformat(),
            },
            priority=100,
        )
        rolling_counts = await queue.get_status_counts(WorkType.ROLLING, chunk_prefix=prefix_like)

    if not _is_stage_complete(rolling_counts):
        return

    # Stage 2: Calibration + Originality (chunked)
    cal_days = params.windows.calibration_window_days
    cal_start, cal_end = get_canonical_window_bounds(cal_days, reference_time=roll_end)
    cal_miners = await database.read(
        _SELECT_MINERS_WITH_OUTCOMES,
        params={"window_start": cal_start, "window_end": cal_end},
        mappings=True,
    )
    cal_ids = [row["miner_id"] for row in cal_miners]
    cal_chunks = _chunk_ranges(cal_ids, MINER_CHUNK_SIZE)

    if cal_chunks:
        cal_counts = await queue.get_status_counts(WorkType.CALIBRATION, chunk_prefix=prefix_like)
        if not cal_counts:
            for miner_min, miner_max in cal_chunks:
                await queue.create_work_batch(
                    WorkType.CALIBRATION,
                    chunk_keys=[f"{prefix}m{miner_min}-{miner_max}"],
                    params={
                        "run_id": run_id,
                        "window_start": cal_start.isoformat(),
                        "window_end": cal_end.isoformat(),
                        "miner_id_min": miner_min,
                        "miner_id_max": miner_max,
                    },
                    priority=50,
                )
            cal_counts = await queue.get_status_counts(WorkType.CALIBRATION, chunk_prefix=prefix_like)
        if not _is_stage_complete(cal_counts):
            return
    else:
        cal_counts = {}

    orig_start, orig_end = get_canonical_window_bounds(rolling_days, reference_time=roll_end)
    orig_miners = await database.read(
        _SELECT_MINERS_WITH_SUBMISSIONS,
        params={"window_start": orig_start, "window_end": orig_end},
        mappings=True,
    )
    orig_ids = [row["miner_id"] for row in orig_miners]
    orig_chunks = _chunk_ranges(orig_ids, MINER_CHUNK_SIZE)

    if orig_chunks:
        orig_counts = await queue.get_status_counts(WorkType.ORIGINALITY, chunk_prefix=prefix_like)
        if not orig_counts:
            for miner_min, miner_max in orig_chunks:
                await queue.create_work_batch(
                    WorkType.ORIGINALITY,
                    chunk_keys=[f"{prefix}m{miner_min}-{miner_max}"],
                    params={
                        "run_id": run_id,
                        "window_start": orig_start.isoformat(),
                        "window_end": orig_end.isoformat(),
                        "miner_id_min": miner_min,
                        "miner_id_max": miner_max,
                    },
                    priority=50,
                )
            orig_counts = await queue.get_status_counts(WorkType.ORIGINALITY, chunk_prefix=prefix_like)
        if not _is_stage_complete(orig_counts):
            return
    else:
        orig_counts = {}

    # Stage 3: Composite Uniqueness (singleton — depends on Originality)
    cu_counts = await queue.get_status_counts(WorkType.COMPOSITE_UNIQUENESS, chunk_prefix=prefix_like)
    if not cu_counts:
        await queue.create_work_batch(
            WorkType.COMPOSITE_UNIQUENESS,
            chunk_keys=[f"{prefix}all"],
            params={
                "run_id": run_id,
                "window_start": orig_start.isoformat(),
                "window_end": orig_end.isoformat(),
            },
            priority=70,
        )
        cu_counts = await queue.get_status_counts(WorkType.COMPOSITE_UNIQUENESS, chunk_prefix=prefix_like)
    if not _is_stage_complete(cu_counts):
        return

    # Stage 4: Shapley Contribution (singleton — runs on settled markets)
    shapley_counts = await queue.get_status_counts(WorkType.SHAPLEY, chunk_prefix=prefix_like)
    if not shapley_counts:
        await queue.create_work_batch(
            WorkType.SHAPLEY,
            chunk_keys=[f"{prefix}all"],
            params={
                "run_id": run_id,
                "as_of": roll_end.isoformat(),
            },
            priority=60,
        )
        shapley_counts = await queue.get_status_counts(WorkType.SHAPLEY, chunk_prefix=prefix_like)
    if not _is_stage_complete(shapley_counts):
        return

    # Stage 5: Skill score (singleton — Cobb-Douglas, depends on all prior stages)
    skill_counts = await queue.get_status_counts(WorkType.SKILL, chunk_prefix=prefix_like)
    if not skill_counts:
        await queue.create_work_batch(
            WorkType.SKILL,
            chunk_keys=[f"{prefix}all"],
            params={
                "run_id": run_id,
                "as_of": roll_end.isoformat(),
            },
            priority=80,
        )
