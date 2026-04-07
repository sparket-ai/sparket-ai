"""Base class for scoring batch jobs.

Provides common functionality for all scoring jobs:
- Automatic checkpointing for crash recovery
- Progress tracking
- Structured logging
- Error handling
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text


class ScoringJob(ABC):
    """Base class for all scoring batch jobs.

    Subclasses must:
    - Set JOB_ID class attribute
    - Implement execute() method

    Features:
    - Automatic checkpointing every CHECKPOINT_INTERVAL items
    - Crash recovery from last checkpoint
    - Progress tracking in scoring_job_state table
    - Structured logging
    """

    # Must be overridden by subclasses
    JOB_ID: str = ""
    CHECKPOINT_INTERVAL: int = 100

    def __init__(
        self,
        db: Any,
        logger: logging.Logger,
        *,
        job_id_override: str | None = None,
    ):
        """Initialize the job.

        Args:
            db: Database manager
            logger: Logger instance
        """
        if not self.JOB_ID and not job_id_override:
            raise ValueError("JOB_ID must be set")

        self.db = db
        self.logger = logger
        self.job_id = job_id_override or self.JOB_ID
        self.state: Dict[str, Any] = {}
        self.items_processed = 0
        self.items_total = 0
        self._started_at: Optional[datetime] = None

    async def run(self) -> None:
        """Main entry point with recovery and checkpointing."""
        try:
            # Load checkpoint if exists
            await self._load_checkpoint()

            # Mark job as running
            await self._update_status("running")
            self._started_at = datetime.now(timezone.utc)

            self.logger.info(f"Starting job {self.job_id}")

            # Execute job logic
            await self.execute()

            # Mark complete
            await self._update_status("completed")
            await self._clear_checkpoint()

            elapsed = (datetime.now(timezone.utc) - self._started_at).total_seconds()
            self.logger.info(
                f"Job {self.job_id} completed: "
                f"processed {self.items_processed}/{self.items_total} in {elapsed:.2f}s"
            )

        except Exception as e:
            self.logger.error(f"Job {self.job_id} failed: {e}", exc_info=True)
            await self._update_status("failed", error=str(e))
            raise

    @abstractmethod
    async def execute(self) -> None:
        """Job-specific logic.

        Subclasses must implement this method.
        Should call self.checkpoint() periodically for long-running jobs.
        """
        pass

    async def checkpoint(self) -> None:
        """Save current state to database for crash recovery."""
        try:
            await self.db.write(
                text(
                    """
                    INSERT INTO scoring_job_state (
                        job_id, status, last_checkpoint, checkpoint_data,
                        items_processed, items_total, started_at, error_count
                    ) VALUES (
                        :job_id, 'running', :ts, :data, :processed, :total, :started, 0
                    )
                    ON CONFLICT (job_id) DO UPDATE SET
                        last_checkpoint = :ts,
                        checkpoint_data = :data,
                        items_processed = :processed
                    """
                ),
                params={
                    "job_id": self.job_id,
                    "ts": datetime.now(timezone.utc),
                    "data": json.dumps(self.state),
                    "processed": self.items_processed,
                    "total": self.items_total,
                    "started": self._started_at,
                },
            )
        except Exception as e:
            self.logger.warning(f"Failed to save checkpoint: {e}")

    async def checkpoint_if_due(self) -> None:
        """Save checkpoint if enough items have been processed."""
        if self.items_processed > 0 and self.items_processed % self.CHECKPOINT_INTERVAL == 0:
            await self.checkpoint()

    async def _load_checkpoint(self) -> None:
        """Restore state from last checkpoint if job was interrupted."""
        try:
            rows = await self.db.read(
                text(
                    """
                    SELECT checkpoint_data, items_processed, started_at
                    FROM scoring_job_state
                    WHERE job_id = :job_id AND status = 'running'
                    """
                ),
                params={"job_id": self.job_id},
                mappings=True,
            )

            if rows:
                row = rows[0]
                checkpoint_data = row["checkpoint_data"]
                if checkpoint_data:
                    if isinstance(checkpoint_data, str):
                        self.state = json.loads(checkpoint_data)
                    else:
                        self.state = checkpoint_data

                self.items_processed = row["items_processed"] or 0
                self._started_at = row["started_at"]

                self.logger.info(
                    f"Restored checkpoint for {self.job_id}: "
                    f"{self.items_processed} items processed"
                )

        except Exception as e:
            self.logger.warning(f"Failed to load checkpoint: {e}")

    async def _update_status(
        self,
        status: str,
        error: Optional[str] = None,
    ) -> None:
        """Update job status in the database."""
        try:
            now = datetime.now(timezone.utc)
            params: Dict[str, Any] = {
                "job_id": self.job_id,
                "status": status,
                "processed": self.items_processed,
                "total": self.items_total,
            }

            if status == "running":
                params["started_at"] = now
                params["checkpoint_data"] = "{}"
                params["error_count"] = 0
                await self.db.write(
                    text(
                        """
                        INSERT INTO scoring_job_state (
                            job_id, status, started_at, items_processed, items_total, checkpoint_data, error_count
                        ) VALUES (
                            :job_id, :status, :started_at, :processed, :total, :checkpoint_data, :error_count
                        )
                        ON CONFLICT (job_id) DO UPDATE SET
                            status = :status,
                            started_at = :started_at,
                            items_processed = :processed,
                            items_total = :total,
                            error_count = 0
                        """
                    ),
                    params=params,
                )
            elif status == "completed":
                params["completed_at"] = now
                await self.db.write(
                    text(
                        """
                        UPDATE scoring_job_state
                        SET status = :status,
                            completed_at = :completed_at,
                            items_processed = :processed,
                            items_total = :total,
                            checkpoint_data = '{}'
                        WHERE job_id = :job_id
                        """
                    ),
                    params=params,
                )
            elif status == "failed":
                params["error"] = error[:1000] if error else None
                await self.db.write(
                    text(
                        """
                        UPDATE scoring_job_state
                        SET status = :status,
                            last_error = :error,
                            error_count = error_count + 1,
                            items_processed = :processed
                        WHERE job_id = :job_id
                        """
                    ),
                    params=params,
                )

        except Exception as e:
            self.logger.warning(f"Failed to update job status: {e}")

    async def _clear_checkpoint(self) -> None:
        """Clear checkpoint data after successful completion."""
        try:
            await self.db.write(
                text(
                    """
                    UPDATE scoring_job_state
                    SET checkpoint_data = '{}',
                        last_checkpoint = NULL
                    WHERE job_id = :job_id
                    """
                ),
                params={"job_id": self.job_id},
            )
        except Exception as e:
            self.logger.warning(f"Failed to clear checkpoint: {e}")


__all__ = ["ScoringJob"]

