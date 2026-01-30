"""Scoring worker main loop.

This is the entry point for the scoring worker process.
It runs independently and handles batch scoring jobs.

Features:
- Independent database connection
- Scheduled job execution
- Heartbeat updates with memory monitoring
- Memory threshold limits with auto-restart
- Stale worker detection and cleanup
- Graceful shutdown handling
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from sparket.validator.scoring.batch.processor import WorkType
from sparket.validator.scoring.batch.work_queue import WorkQueue
from sparket.validator.scoring.worker import db_ops

# Configure logging for worker process
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scoring_worker")

# Memory limit configuration (MB)
DEFAULT_MEMORY_LIMIT_MB = 2048
MEMORY_WARNING_THRESHOLD_MB = 1536

# Stale worker threshold
STALE_HEARTBEAT_MINUTES = 5

WORK_TYPE_ORDER = (
    WorkType.ROLLING,
    WorkType.CALIBRATION,
    WorkType.ORIGINALITY,
    WorkType.SKILL,
)


class ScoringWorkerRunner:
    """Main loop for scoring worker process.

    Features:
    - Independent database connection
    - Scheduled job execution
    - Heartbeat updates
    - Memory monitoring with auto-restart
    - Stale worker cleanup
    - Graceful shutdown handling
    """

    def __init__(self, config: Any, worker_id: str):
        """Initialize the worker runner.

        Args:
            config: Validator configuration
            worker_id: Unique identifier for this worker
        """
        self.config = config
        self.worker_id = worker_id
        self.db: Optional[Any] = None
        self.work_queue: Optional[WorkQueue] = None
        self._shutdown_requested = False
        self._current_job: Optional[str] = None
        self._memory_limit_mb = DEFAULT_MEMORY_LIMIT_MB
        self._startup_memory_mb: Optional[int] = None
        self.heartbeat_timeout_sec = 60
        self.stale_work_minutes = 30

        # Get worker params
        try:
            from sparket.validator.config.scoring_params import get_scoring_params
            params = get_scoring_params()
            self.heartbeat_interval = params.worker.heartbeat_interval_sec
            self.batch_interval_hours = params.worker.batch_interval_hours
            self.heartbeat_timeout_sec = params.worker.heartbeat_timeout_sec
            self.stale_work_minutes = max(
                1,
                int((self.heartbeat_timeout_sec / 60) * 2),
            )
            # Allow memory limit override from config
            if hasattr(params.worker, 'memory_limit_mb'):
                self._memory_limit_mb = params.worker.memory_limit_mb
        except Exception:
            self.heartbeat_interval = 10
            self.batch_interval_hours = 6

    def run(self) -> None:
        """Main worker entry point."""
        logger.info(f"Scoring worker {self.worker_id} starting (PID: {os.getpid()})")
        logger.info(f"Memory limit: {self._memory_limit_mb} MB")

        # Register signal handlers
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        try:
            # Create a single event loop for the entire worker lifecycle
            # This prevents "Future attached to a different loop" errors
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Create independent database connection
                self._init_database()

                # Record startup memory baseline
                self._startup_memory_mb = self._get_memory_mb()
                logger.info(f"Startup memory: {self._startup_memory_mb} MB")

                # Run startup tasks in the same loop
                loop.run_until_complete(self._startup_tasks())

                # Main loop
                loop.run_until_complete(self._main_loop())
            finally:
                loop.run_until_complete(self._cleanup())
                loop.close()

        except Exception as e:
            logger.error(f"Worker crashed: {e}", exc_info=True)
            sys.exit(1)

        # Check exit reason
        if self._shutdown_requested:
            logger.info(f"Scoring worker {self.worker_id} stopped (graceful shutdown)")
        else:
            logger.warning(f"Scoring worker {self.worker_id} stopped (memory limit)")
            sys.exit(2)  # Special exit code for memory limit

    async def _startup_tasks(self) -> None:
        """Run startup tasks."""
        # Register worker
        await self._register_worker()
        
        # Clean up stale workers
        await self._cleanup_stale_workers()
        
        # Reset any stuck jobs
        await self._reset_stuck_jobs()

    def _init_database(self) -> None:
        """Initialize database connection."""
        from sparket.validator.database.dbm import DBM

        self.db = DBM.create_worker(self.config, pool_size=10)
        self.work_queue = WorkQueue(self.db, worker_id=self.worker_id)
        logger.info("Database connection established")

    def _handle_signal(self, signum: int, frame: Any) -> None:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received {signal_name}, initiating shutdown...")
        self._shutdown_requested = True

    async def _register_worker(self) -> None:
        """Register this worker in the heartbeat table."""
        await db_ops.register_worker(
            self.db,
            self.worker_id,
            self._startup_memory_mb or 0,
        )
        logger.info(f"Registered worker {self.worker_id}")

    async def _cleanup_stale_workers(self) -> None:
        """Clean up workers with stale heartbeats and release their work."""
        try:
            await db_ops.cleanup_stale_workers(
                self.db,
                self.worker_id,
                STALE_HEARTBEAT_MINUTES,
            )
        except Exception as e:
            logger.warning(f"Failed to cleanup stale workers: {e}")

    async def _reset_stuck_jobs(self) -> None:
        """Reset jobs that are stuck in 'running' state from previous crashes."""
        try:
            await db_ops.reset_stuck_jobs(self.db, threshold_hours=2)
        except Exception as e:
            logger.warning(f"Failed to reset stuck jobs: {e}")

    async def _main_loop(self) -> None:
        """Main worker loop."""
        while not self._shutdown_requested:
            try:
                # Check memory limit
                if self._check_memory_limit():
                    logger.warning("Memory limit exceeded, requesting shutdown for restart")
                    break
                
                # Update heartbeat
                await self._update_heartbeat()

                # Reclaim stale work
                if self.work_queue is not None:
                    await self.work_queue.reclaim_stale_work(
                        stale_minutes=self.stale_work_minutes
                    )

                # Claim and process work
                work = await self._claim_next_work()
                if work is not None:
                    work_type, work_item = work
                    await self._run_work_item(work_type, work_item)
                    continue

                # Sleep between idle iterations
                await asyncio.sleep(self.heartbeat_interval)

            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(self.heartbeat_interval)

    def _check_memory_limit(self) -> bool:
        """Check if memory usage exceeds limit.
        
        Returns:
            True if memory limit exceeded and worker should restart
        """
        memory_mb = self._get_memory_mb()
        
        # Warning threshold
        if memory_mb > MEMORY_WARNING_THRESHOLD_MB:
            logger.warning(f"Memory usage high: {memory_mb} MB (warning: {MEMORY_WARNING_THRESHOLD_MB} MB)")
        
        # Hard limit
        if memory_mb > self._memory_limit_mb:
            logger.error(f"Memory limit exceeded: {memory_mb} MB > {self._memory_limit_mb} MB")
            return True
        
        return False

    async def _update_heartbeat(self) -> None:
        """Update heartbeat timestamp."""
        try:
            memory_mb = self._get_memory_mb()
            await db_ops.update_heartbeat(
                self.db,
                self.worker_id,
                self._current_job,
                memory_mb,
            )
        except Exception as e:
            logger.warning(f"Failed to update heartbeat: {e}")

    def _get_memory_mb(self) -> int:
        """Get current memory usage in MB."""
        try:
            # Prefer current RSS from /proc if available
            try:
                with open("/proc/self/statm", "r", encoding="utf-8") as handle:
                    parts = handle.read().strip().split()
                if len(parts) >= 2:
                    rss_pages = int(parts[1])
                    page_size = os.sysconf("SC_PAGE_SIZE")
                    return int((rss_pages * page_size) / (1024 * 1024))
            except Exception:
                pass
            import resource
            usage = resource.getrusage(resource.RUSAGE_SELF)
            return int(usage.ru_maxrss / 1024)  # Convert KB to MB
        except Exception:
            return 0

    async def _claim_next_work(
        self,
    ) -> Optional[Tuple[WorkType, Dict[str, Any]]]:
        """Claim the next available work item in priority order."""
        if self.work_queue is None:
            return None

        for work_type in WORK_TYPE_ORDER:
            item = await self.work_queue.claim_work(work_type)
            if item:
                return work_type, item

        return None

    def _make_job_id(self, base: str, chunk_key: str) -> str:
        """Build a compact job ID for per-chunk checkpointing."""
        digest = hashlib.sha1(chunk_key.encode("utf-8")).hexdigest()[:8]
        job_id = f"{base}:{digest}"
        return job_id[:64]

    def _parse_dt(self, value: Any) -> Optional[datetime]:
        """Parse an ISO datetime string to a timezone-aware datetime."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None

    async def _run_work_item(self, work_type: WorkType, item: Dict[str, Any]) -> None:
        """Dispatch a claimed work item to the correct scoring job."""
        if self.db is None or self.work_queue is None:
            return

        work_id = item["work_id"]
        chunk_key = item["chunk_key"]
        params = item.get("params") or {}
        job_id = self._make_job_id("work", chunk_key)
        self._current_job = job_id
        try:
            window_start = self._parse_dt(params.get("window_start"))
            window_end = self._parse_dt(params.get("window_end"))
            as_of = self._parse_dt(params.get("as_of"))
            miner_id_min = params.get("miner_id_min")
            miner_id_max = params.get("miner_id_max")
            if miner_id_min is not None:
                miner_id_min = int(miner_id_min)
            if miner_id_max is not None:
                miner_id_max = int(miner_id_max)

            if work_type == WorkType.ROLLING:
                from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob

                job_id = self._make_job_id(RollingAggregatesJob.JOB_ID, chunk_key)
                self._current_job = job_id
                job = RollingAggregatesJob(
                    self.db,
                    logger,
                    window_start=window_start,
                    window_end=window_end,
                    job_id_override=job_id,
                )
            elif work_type == WorkType.CALIBRATION:
                from sparket.validator.scoring.jobs.calibration_sharpness import (
                    CalibrationSharpnessJob,
                )

                job_id = self._make_job_id(CalibrationSharpnessJob.JOB_ID, chunk_key)
                self._current_job = job_id
                job = CalibrationSharpnessJob(
                    self.db,
                    logger,
                    window_start=window_start,
                    window_end=window_end,
                    miner_id_min=miner_id_min,
                    miner_id_max=miner_id_max,
                    job_id_override=job_id,
                )
            elif work_type == WorkType.ORIGINALITY:
                from sparket.validator.scoring.jobs.originality_lead_lag import (
                    OriginalityLeadLagJob,
                )

                job_id = self._make_job_id(OriginalityLeadLagJob.JOB_ID, chunk_key)
                self._current_job = job_id
                job = OriginalityLeadLagJob(
                    self.db,
                    logger,
                    window_start=window_start,
                    window_end=window_end,
                    miner_id_min=miner_id_min,
                    miner_id_max=miner_id_max,
                    job_id_override=job_id,
                )
            elif work_type == WorkType.SKILL:
                from sparket.validator.scoring.jobs.skill_score import SkillScoreJob

                job_id = self._make_job_id(SkillScoreJob.JOB_ID, chunk_key)
                self._current_job = job_id
                job = SkillScoreJob(
                    self.db,
                    logger,
                    as_of=as_of,
                    job_id_override=job_id,
                )
            else:
                logger.warning(f"Unknown work type: {work_type}")
                await self.work_queue.fail_work(work_id, f"Unknown work type: {work_type}")
                return

            logger.info(f"Starting work {work_id} ({work_type.value})")
            start_time = time.time()
            await job.run()
            elapsed = time.time() - start_time
            logger.info(f"Work {work_id} completed in {elapsed:.2f}s")

            await self._record_job_success(job_id)
            await self.work_queue.complete_work(
                work_id,
                result={
                    "job_id": job_id,
                    "items_processed": job.items_processed,
                    "items_total": job.items_total,
                },
            )
        except Exception as e:
            logger.error(f"Work {work_id} failed: {e}", exc_info=True)
            await self._record_job_failure(job_id, str(e))
            await self.work_queue.fail_work(work_id, str(e))
        finally:
            self._current_job = None

    async def _record_job_success(self, job_name: str) -> None:
        """Record successful job completion."""
        try:
            await db_ops.record_job_success(self.db, self.worker_id)
        except Exception as e:
            logger.warning(f"Failed to record job success: {e}")

    async def _record_job_failure(self, job_name: str, error: str) -> None:
        """Record job failure."""
        try:
            await db_ops.record_job_failure(self.db, self.worker_id, job_name, error)
        except Exception as e:
            logger.warning(f"Failed to record job failure: {e}")

    async def _cleanup(self) -> None:
        """Clean up on shutdown."""
        logger.info("Cleaning up worker resources")

        try:
            await db_ops.clear_current_job(self.db, self.worker_id)
        except Exception as e:
            logger.warning(f"Failed to cleanup heartbeat: {e}")

        try:
            if self.db:
                await self.db.dispose()
        except Exception as e:
            logger.warning(f"Failed to dispose database: {e}")


def worker_main(config: Any, worker_id: str) -> None:
    """Entry point for worker process.

    This function is called by multiprocessing.Process.

    Args:
        config: Validator configuration
        worker_id: Unique worker identifier
    """
    runner = ScoringWorkerRunner(config, worker_id)
    runner.run()


__all__ = ["ScoringWorkerRunner", "worker_main"]
