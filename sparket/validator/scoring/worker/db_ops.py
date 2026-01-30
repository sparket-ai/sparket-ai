"""Database operations for scoring worker processes."""

from __future__ import annotations

import logging
import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

logger = logging.getLogger("scoring_worker.db_ops")


async def register_worker(db: Any, worker_id: str, memory_mb: int) -> None:
    """Register a worker heartbeat row."""
    now = datetime.now(timezone.utc)
    hostname = socket.gethostname()

    await db.write(
        text(
            """
            INSERT INTO scoring_worker_heartbeat (
                worker_id, pid, hostname, started_at, last_heartbeat,
                jobs_completed, jobs_failed, memory_mb
            ) VALUES (
                :worker_id, :pid, :hostname, :started_at, :last_heartbeat, 0, 0, :memory_mb
            )
            ON CONFLICT (worker_id) DO UPDATE SET
                pid = :pid,
                hostname = :hostname,
                started_at = :started_at,
                last_heartbeat = :last_heartbeat,
                current_job = NULL,
                memory_mb = :memory_mb
            """
        ),
        params={
            "worker_id": worker_id,
            "pid": os.getpid(),
            "hostname": hostname,
            "started_at": now,
            "last_heartbeat": now,
            "memory_mb": memory_mb,
        },
    )


async def update_heartbeat(
    db: Any,
    worker_id: str,
    current_job: str | None,
    memory_mb: int,
) -> None:
    """Update heartbeat timestamp and job info."""
    await db.write(
        text(
            """
            UPDATE scoring_worker_heartbeat
            SET last_heartbeat = :ts, current_job = :job, memory_mb = :mem
            WHERE worker_id = :worker_id
            """
        ),
        params={
            "worker_id": worker_id,
            "ts": datetime.now(timezone.utc),
            "job": current_job,
            "mem": memory_mb,
        },
    )


async def record_job_success(db: Any, worker_id: str) -> None:
    """Record successful job completion."""
    await db.write(
        text(
            """
            UPDATE scoring_worker_heartbeat
            SET jobs_completed = jobs_completed + 1
            WHERE worker_id = :worker_id
            """
        ),
        params={"worker_id": worker_id},
    )


async def record_job_failure(db: Any, worker_id: str, job_id: str, error: str) -> None:
    """Record job failure."""
    await db.write(
        text(
            """
            UPDATE scoring_worker_heartbeat
            SET jobs_failed = jobs_failed + 1
            WHERE worker_id = :worker_id
            """
        ),
        params={"worker_id": worker_id},
    )

    await db.write(
        text(
            """
            INSERT INTO scoring_job_state (job_id, status, last_error, error_count, checkpoint_data)
            VALUES (:job_id, 'failed', :error, 1, '{}')
            ON CONFLICT (job_id) DO UPDATE SET
                status = 'failed',
                last_error = :error,
                error_count = scoring_job_state.error_count + 1
            """
        ),
        params={"job_id": job_id, "error": error[:1000]},
    )


async def clear_current_job(db: Any, worker_id: str) -> None:
    """Clear current job from heartbeat table."""
    await db.write(
        text(
            """
            UPDATE scoring_worker_heartbeat
            SET current_job = NULL
            WHERE worker_id = :worker_id
            """
        ),
        params={"worker_id": worker_id},
    )


async def cleanup_stale_workers(
    db: Any,
    current_worker_id: str,
    stale_minutes: int,
) -> None:
    """Clean up workers with stale heartbeats and release their work."""
    stale_threshold = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)

    stale_workers = await db.read(
        text(
            """
            SELECT worker_id, last_heartbeat, current_job
            FROM scoring_worker_heartbeat
            WHERE last_heartbeat < :threshold
              AND worker_id != :current_worker
            """
        ),
        params={
            "threshold": stale_threshold,
            "current_worker": current_worker_id,
        },
        mappings=True,
    )

    if not stale_workers:
        return

    logger.info(f"Found {len(stale_workers)} stale workers to clean up")

    for worker in stale_workers:
        worker_id = worker["worker_id"]
        current_job = worker.get("current_job")

        logger.warning(
            f"Cleaning up stale worker: {worker_id} (last seen: {worker['last_heartbeat']})"
        )

        await db.write(
            text(
                """
                UPDATE scoring_work_queue
                SET status = 'pending', 
                    claimed_by = NULL,
                    claimed_at = NULL,
                    retry_count = retry_count + 1
                WHERE claimed_by = :worker_id
                  AND status = 'claimed'
                """
            ),
            params={"worker_id": worker_id},
        )

        if current_job:
            await db.write(
                text(
                    """
                    UPDATE scoring_job_state
                    SET status = 'pending',
                        last_error = 'Worker stale - requeued'
                    WHERE job_id = :job_id
                      AND status = 'running'
                    """
                ),
                params={"job_id": current_job},
            )
            logger.info(f"Reset stuck job: {current_job}")
        
        # Delete the stale worker entry from heartbeat table
        await db.write(
            text(
                """
                DELETE FROM scoring_worker_heartbeat
                WHERE worker_id = :worker_id
                """
            ),
            params={"worker_id": worker_id},
        )
        logger.info(f"Removed stale worker entry: {worker_id}")


async def reset_stuck_jobs(db: Any, *, threshold_hours: int = 2) -> None:
    """Reset jobs stuck in running state from previous crashes."""
    stuck_threshold = datetime.now(timezone.utc) - timedelta(hours=threshold_hours)

    result = await db.write(
        text(
            """
            UPDATE scoring_job_state
            SET status = 'pending',
                last_error = 'Reset from stuck running state'
            WHERE status = 'running'
              AND started_at < :threshold
            RETURNING job_id
            """
        ),
        params={"threshold": stuck_threshold},
        return_rows=True,
    )

    if result:
        job_ids = [r[0] for r in result]
        logger.info(f"Reset {len(job_ids)} stuck jobs: {job_ids}")
