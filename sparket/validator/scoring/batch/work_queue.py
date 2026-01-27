"""Database-backed work queue for parallel scoring workers.

Provides:
- Atomic work claiming with row-level locking
- Chunked work distribution across workers
- Failure recovery and retry logic

Each work item represents a chunk of scoring work that can be
processed independently by any worker.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from .processor import WorkType


class WorkStatus(str, Enum):
    """Work item status."""
    PENDING = "pending"
    CLAIMED = "claimed"
    COMPLETED = "completed"
    FAILED = "failed"


# SQL for work queue operations
_CREATE_WORK_BATCH = text("""
    INSERT INTO scoring_work_queue (
        work_id, work_type, chunk_key, params, priority, created_at, status, retry_count
    )
    SELECT 
        gen_random_uuid(),
        :work_type,
        unnest(CAST(:chunk_keys AS text[])),
        :params,
        :priority,
        :created_at,
        'pending',
        :retry_count
    ON CONFLICT (work_type, chunk_key) DO NOTHING
    RETURNING work_id
""")

_CLAIM_WORK = text("""
    UPDATE scoring_work_queue
    SET 
        status = 'claimed',
        claimed_by = :worker_id,
        claimed_at = :claimed_at
    WHERE work_id = (
        SELECT work_id FROM scoring_work_queue
        WHERE work_type = :work_type
          AND status = 'pending'
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    )
    RETURNING work_id, chunk_key, params
""")

_CLAIM_WORK_BATCH = text("""
    UPDATE scoring_work_queue
    SET 
        status = 'claimed',
        claimed_by = :worker_id,
        claimed_at = :claimed_at
    WHERE work_id IN (
        SELECT work_id FROM scoring_work_queue
        WHERE work_type = :work_type
          AND status = 'pending'
        ORDER BY priority DESC, created_at ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
    RETURNING work_id, chunk_key, params
""")

_COMPLETE_WORK = text("""
    UPDATE scoring_work_queue
    SET 
        status = 'completed',
        completed_at = :completed_at,
        result = :result
    WHERE work_id = :work_id
      AND status = 'claimed'
      AND claimed_by = :worker_id
""")

_FAIL_WORK = text("""
    UPDATE scoring_work_queue
    SET 
        status = 'failed',
        completed_at = :failed_at,
        error = :error,
        retry_count = retry_count + 1
    WHERE work_id = :work_id
      AND status = 'claimed'
""")

_RECLAIM_STALE = text("""
    UPDATE scoring_work_queue
    SET 
        status = 'pending',
        claimed_by = NULL,
        claimed_at = NULL
    WHERE status = 'claimed'
      AND claimed_at < :stale_threshold
    RETURNING work_id
""")

_GET_QUEUE_STATS = text("""
    SELECT 
        work_type,
        status,
        COUNT(*) as count
    FROM scoring_work_queue
    WHERE created_at > :since
    GROUP BY work_type, status
""")

_GET_STATUS_COUNTS = text("""
    SELECT
        status,
        COUNT(*) as count
    FROM scoring_work_queue
    WHERE work_type = :work_type
      AND chunk_key LIKE :chunk_prefix
    GROUP BY status
""")

_CLEANUP_OLD_COMPLETED = text("""
    DELETE FROM scoring_work_queue
    WHERE status IN ('completed', 'failed')
      AND completed_at < :threshold
    RETURNING work_id
""")


class WorkQueue:
    """Database-backed work queue for distributed scoring.
    
    Thread-safe and multi-worker safe using row-level locking.
    """
    
    def __init__(self, db: Any, worker_id: Optional[str] = None):
        """Initialize work queue.
        
        Args:
            db: Database connection manager
            worker_id: Unique identifier for this worker (auto-generated if None)
        """
        self.db = db
        self.worker_id = worker_id or str(uuid.uuid4())[:8]
    
    async def create_work_batch(
        self,
        work_type: WorkType,
        chunk_keys: List[str],
        params: Dict[str, Any],
        priority: int = 0,
    ) -> List[str]:
        """Create a batch of work items.
        
        Args:
            work_type: Type of work
            chunk_keys: List of chunk identifiers
            params: Shared parameters for all chunks
            priority: Higher = processed first
            
        Returns:
            List of created work_ids
        """
        import json
        
        result = await self.db.write(
            _CREATE_WORK_BATCH,
            params={
                "work_type": work_type.value,
                "chunk_keys": chunk_keys,
                "params": json.dumps(params),
                "priority": priority,
                "created_at": datetime.now(timezone.utc),
                "retry_count": 0,
            },
            return_rows=True,
            mappings=True,
        )
        
        return [str(row["work_id"]) for row in result] if result else []
    
    async def claim_work(
        self,
        work_type: WorkType,
    ) -> Optional[Dict[str, Any]]:
        """Claim a single work item atomically.
        
        Uses FOR UPDATE SKIP LOCKED to prevent race conditions.
        
        Returns:
            Work item dict with work_id, chunk_key, params, or None if no work
        """
        import json
        
        result = await self.db.write(
            _CLAIM_WORK,
            params={
                "work_type": work_type.value,
                "worker_id": self.worker_id,
                "claimed_at": datetime.now(timezone.utc),
            },
            return_rows=True,
            mappings=True,
        )
        
        if not result:
            return None
        
        row = result[0]
        return {
            "work_id": str(row["work_id"]),
            "chunk_key": row["chunk_key"],
            "params": json.loads(row["params"]) if row["params"] else {},
        }
    
    async def claim_work_batch(
        self,
        work_type: WorkType,
        batch_size: int = 10,
    ) -> List[Dict[str, Any]]:
        """Claim multiple work items atomically.
        
        More efficient than claiming one at a time.
        
        Returns:
            List of work items
        """
        import json
        
        result = await self.db.write(
            _CLAIM_WORK_BATCH,
            params={
                "work_type": work_type.value,
                "worker_id": self.worker_id,
                "claimed_at": datetime.now(timezone.utc),
                "batch_size": batch_size,
            },
            return_rows=True,
            mappings=True,
        )
        
        if not result:
            return []
        
        return [
            {
                "work_id": str(row["work_id"]),
                "chunk_key": row["chunk_key"],
                "params": json.loads(row["params"]) if row["params"] else {},
            }
            for row in result
        ]
    
    async def complete_work(
        self,
        work_id: str,
        result: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Mark work as completed.
        
        Args:
            work_id: Work item ID
            result: Optional result data to store
            
        Returns:
            True if marked complete, False if not found/not claimed by us
        """
        import json
        
        await self.db.write(
            _COMPLETE_WORK,
            params={
                "work_id": work_id,
                "worker_id": self.worker_id,
                "completed_at": datetime.now(timezone.utc),
                "result": json.dumps(result) if result else None,
            },
        )
        return True
    
    async def fail_work(
        self,
        work_id: str,
        error: str,
    ) -> None:
        """Mark work as failed.
        
        Args:
            work_id: Work item ID
            error: Error message
        """
        await self.db.write(
            _FAIL_WORK,
            params={
                "work_id": work_id,
                "failed_at": datetime.now(timezone.utc),
                "error": error[:1000],  # Truncate long errors
            },
        )
    
    async def reclaim_stale_work(
        self,
        stale_minutes: int = 30,
    ) -> int:
        """Reclaim work items that were claimed but never completed.
        
        This handles worker crashes/restarts.
        
        Args:
            stale_minutes: Minutes after which claimed work is considered stale
            
        Returns:
            Number of items reclaimed
        """
        threshold = datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)
        
        result = await self.db.write(
            _RECLAIM_STALE,
            params={"stale_threshold": threshold},
            return_rows=True,
            mappings=True,
        )
        
        return len(result) if result else 0
    
    async def get_queue_stats(
        self,
        hours: int = 24,
    ) -> Dict[str, Dict[str, int]]:
        """Get queue statistics.
        
        Returns:
            Dict mapping work_type -> status -> count
        """
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        result = await self.db.read(
            _GET_QUEUE_STATS,
            params={"since": since},
            mappings=True,
        )
        
        stats: Dict[str, Dict[str, int]] = {}
        for row in result:
            wt = row["work_type"]
            status = row["status"]
            count = row["count"]
            
            if wt not in stats:
                stats[wt] = {}
            stats[wt][status] = count
        
        return stats

    async def get_status_counts(
        self,
        work_type: WorkType,
        chunk_prefix: str = "%",
    ) -> Dict[str, int]:
        """Get status counts for a work type and chunk prefix."""
        result = await self.db.read(
            _GET_STATUS_COUNTS,
            params={"work_type": work_type.value, "chunk_prefix": chunk_prefix},
            mappings=True,
        )
        return {row["status"]: row["count"] for row in result}
    
    async def cleanup_old(
        self,
        days: int = 7,
    ) -> int:
        """Clean up old completed/failed work items.
        
        Args:
            days: Age threshold for cleanup
            
        Returns:
            Number of items deleted
        """
        threshold = datetime.now(timezone.utc) - timedelta(days=days)
        
        rows = await self.db.write(
            _CLEANUP_OLD_COMPLETED,
            params={"threshold": threshold},
            return_rows=True,
            mappings=True,
        )
        return len(rows or [])


async def create_miner_chunks(
    db: Any,
    window_start: datetime,
    window_end: datetime,
    chunk_size: int = 100,
) -> List[str]:
    """Create miner ID chunks for parallel processing.
    
    Args:
        db: Database connection
        window_start: Window start
        window_end: Window end
        chunk_size: Miners per chunk
        
    Returns:
        List of chunk keys (e.g., "0-99", "100-199")
    """
    query = text("""
        SELECT DISTINCT miner_id
        FROM miner_submission
        WHERE submitted_at >= :window_start
          AND submitted_at < :window_end
        ORDER BY miner_id
    """)
    
    result = await db.read(
        query,
        params={"window_start": window_start, "window_end": window_end},
        mappings=True,
    )
    
    miner_ids = [row["miner_id"] for row in result]
    
    # Create chunks
    chunks = []
    for i in range(0, len(miner_ids), chunk_size):
        chunk_miners = miner_ids[i:i + chunk_size]
        # Use first-last as chunk key
        chunks.append(f"{chunk_miners[0]}-{chunk_miners[-1]}")
    
    return chunks


async def create_market_chunks(
    db: Any,
    market_ids: List[int],
    chunk_size: int = 50,
) -> List[str]:
    """Create market ID chunks for parallel processing.
    
    Args:
        db: Database connection (unused but kept for consistency)
        market_ids: List of market IDs to process
        chunk_size: Markets per chunk
        
    Returns:
        List of chunk keys
    """
    chunks = []
    for i in range(0, len(market_ids), chunk_size):
        chunk_markets = market_ids[i:i + chunk_size]
        chunks.append(f"m{chunk_markets[0]}-{chunk_markets[-1]}")
    
    return chunks


__all__ = [
    "WorkStatus",
    "WorkQueue",
    "create_miner_chunks",
    "create_market_chunks",
]
