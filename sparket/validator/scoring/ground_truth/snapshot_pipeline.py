"""Ground truth snapshot pipeline.

Periodically captures consensus snapshots from sportsbook quotes,
enabling fair time-matched comparison of miner predictions.

Design:
- Run every N hours (configurable, default 6)
- For each active market with book quotes, compute consensus
- Store snapshots in ground_truth_snapshot table
- Mark final closing snapshot when event starts
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from numpy.typing import NDArray
from sqlalchemy import text

from sparket.validator.config.scoring_params import ScoringParams, get_scoring_params

from ..determinism import round_decimal, safe_divide
from .bias import BiasEstimator, BiasKey, BiasState, make_bias_key
from .consensus import BookQuote, ConsensusComputer


@dataclass
class SnapshotResult:
    """Result of a snapshot computation for one market/side."""
    market_id: int
    side: str
    snapshot_ts: datetime
    prob_consensus: Decimal
    odds_consensus: Decimal
    contributing_books: int
    std_dev: Optional[Decimal]
    bias_version: int
    is_closing: bool


# SQL queries
_SELECT_ACTIVE_MARKETS = text("""
    SELECT DISTINCT m.market_id, e.start_time_utc
    FROM market m
    JOIN event e ON m.event_id = e.event_id
    WHERE e.start_time_utc > :now
      AND e.start_time_utc < :max_future
      AND e.status = 'scheduled'
    ORDER BY e.start_time_utc
""")

_SELECT_MARKET_QUOTES = text("""
    SELECT 
        pq.quote_id,
        COALESCE(s.sportsbook_id, 1) as sportsbook_id,
        l.sport_id,
        m.kind as market_kind,
        pq.side,
        pq.imp_prob,
        pq.odds_eu,
        pq.ts
    FROM provider_quote pq
    JOIN market m ON pq.market_id = m.market_id
    JOIN event e ON m.event_id = e.event_id
    JOIN league l ON e.league_id = l.league_id
    LEFT JOIN sportsbook s ON s.provider_id = pq.provider_id
        AND s.code = pq.raw->>'sportsbook'
    WHERE pq.market_id = :market_id
      AND pq.ts >= :since
      AND pq.ts < :until
    ORDER BY pq.ts DESC
""")

_SELECT_LATEST_BIAS = text("""
    SELECT 
        sportsbook_id, sport_id, market_kind,
        bias_factor, variance, sample_count, version
    FROM sportsbook_bias
    WHERE version = (SELECT MAX(version) FROM sportsbook_bias)
""")

_UPSERT_SNAPSHOT = text("""
    INSERT INTO ground_truth_snapshot (
        market_id, side, snapshot_ts,
        prob_consensus, odds_consensus, contributing_books,
        std_dev, bias_version, is_closing
    ) VALUES (
        :market_id, :side, :snapshot_ts,
        :prob_consensus, :odds_consensus, :contributing_books,
        :std_dev, :bias_version, :is_closing
    )
    ON CONFLICT (market_id, side, snapshot_ts) DO UPDATE SET
        prob_consensus = EXCLUDED.prob_consensus,
        odds_consensus = EXCLUDED.odds_consensus,
        contributing_books = EXCLUDED.contributing_books,
        std_dev = EXCLUDED.std_dev,
        bias_version = EXCLUDED.bias_version,
        is_closing = EXCLUDED.is_closing
""")

_DELETE_EXCESS_SNAPSHOTS = text("""
    DELETE FROM ground_truth_snapshot
    WHERE (market_id, side, snapshot_ts) IN (
        SELECT market_id, side, snapshot_ts
        FROM (
            SELECT
                market_id,
                side,
                snapshot_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY market_id, side
                    ORDER BY snapshot_ts DESC
                ) AS rn
            FROM ground_truth_snapshot
            WHERE market_id = :market_id
              AND side = :side
        ) ranked
        WHERE ranked.rn > :max_snapshots
    )
""")

_SELECT_MARKETS_STARTING_SOON = text("""
    SELECT DISTINCT m.market_id, e.start_time_utc
    FROM market m
    JOIN event e ON m.event_id = e.event_id
    WHERE e.start_time_utc > :now
      AND e.start_time_utc <= :threshold
      AND e.status IN ('scheduled', 'in_play')
      AND NOT EXISTS (
          SELECT 1 FROM ground_truth_snapshot gs
          WHERE gs.market_id = m.market_id
            AND gs.is_closing = true
      )
""")

# Markets that have started but never got a closing snapshot (recovery case)
_SELECT_MARKETS_MISSED_CLOSING = text("""
    SELECT DISTINCT m.market_id, e.start_time_utc, e.event_id
    FROM market m
    JOIN event e ON m.event_id = e.event_id
    WHERE e.start_time_utc < :now
      AND e.start_time_utc > :min_time
      AND e.status IN ('scheduled', 'in_play', 'finished')
      AND NOT EXISTS (
          SELECT 1 FROM ground_truth_snapshot gs
          WHERE gs.market_id = m.market_id
            AND gs.is_closing = true
      )
      AND EXISTS (
          SELECT 1 FROM miner_submission ms
          WHERE ms.market_id = m.market_id
      )
    ORDER BY e.start_time_utc DESC
    LIMIT :limit
""")

# Copy closing snapshots to ground_truth_closing for scoring
_UPSERT_GROUND_TRUTH_CLOSING = text("""
    INSERT INTO ground_truth_closing (
        market_id, side, prob_consensus, odds_consensus,
        contributing_books, min_prob, max_prob, std_dev,
        bias_version, computed_at
    )
    SELECT 
        gs.market_id, gs.side, gs.prob_consensus, gs.odds_consensus,
        gs.contributing_books, NULL, NULL, gs.std_dev,
        gs.bias_version, gs.snapshot_ts
    FROM ground_truth_snapshot gs
    WHERE gs.market_id = :market_id
      AND gs.is_closing = true
    ON CONFLICT (market_id, side) DO UPDATE SET
        prob_consensus = EXCLUDED.prob_consensus,
        odds_consensus = EXCLUDED.odds_consensus,
        contributing_books = EXCLUDED.contributing_books,
        std_dev = EXCLUDED.std_dev,
        bias_version = EXCLUDED.bias_version,
        computed_at = EXCLUDED.computed_at
""")

_SELECT_MARKETS_NEEDING_CLOSING = text("""
    SELECT DISTINCT gs.market_id
    FROM ground_truth_snapshot gs
    WHERE gs.is_closing = true
      AND NOT EXISTS (
          SELECT 1 FROM ground_truth_closing gtc
          WHERE gtc.market_id = gs.market_id
      )
""")


class SnapshotPipeline:
    """Manages periodic ground truth snapshot capture.
    
    Designed for parallel execution:
    - Uses work queue for market batches
    - Vectorized quote processing
    - Batch inserts for snapshots
    """
    
    def __init__(
        self,
        db: Any,
        logger: Any,
        params: ScoringParams | None = None,
    ):
        self.db = db
        self.logger = logger
        self.params = params or get_scoring_params()
        self.consensus = ConsensusComputer(self.params)
        self.bias_estimator = BiasEstimator(self.params)
    
    async def run_snapshot_cycle(self) -> int:
        """Run a full snapshot cycle for all active markets.
        
        Returns:
            Number of snapshots created
        """
        now = datetime.now(timezone.utc)
        
        # Load current bias states
        bias_states = await self._load_bias_states()
        bias_version = max((s.version for s in bias_states.values()), default=1)
        
        # Get active markets
        max_future = now + timedelta(days=7)
        markets = await self.db.read(
            _SELECT_ACTIVE_MARKETS,
            params={"now": now, "max_future": max_future},
            mappings=True,
        )
        
        self.logger.info(f"Processing {len(markets)} active markets for snapshots")
        
        snapshot_count = 0
        
        for market in markets:
            market_id = market["market_id"]
            event_start = market["start_time_utc"]
            
            # Check if this is close to event start (closing snapshot)
            minutes_to_start = (event_start - now).total_seconds() / 60
            is_closing = minutes_to_start <= 30  # Within 30 mins of start
            
            snapshots = await self._compute_market_snapshots(
                market_id=market_id,
                snapshot_ts=now,
                bias_states=bias_states,
                bias_version=bias_version,
                is_closing=is_closing,
            )
            
            if snapshots:
                await self._persist_snapshots(snapshots)
                snapshot_count += len(snapshots)
        
        self.logger.info(f"Created {snapshot_count} snapshots")
        return snapshot_count
    
    async def capture_closing_snapshots(self) -> int:
        """Capture closing snapshots for markets about to start.
        
        Should be called frequently (every few minutes) to catch
        final closing lines for events starting soon.
        
        Returns:
            Number of closing snapshots created
        """
        now = datetime.now(timezone.utc)
        threshold = now + timedelta(minutes=30)
        
        bias_states = await self._load_bias_states()
        bias_version = max((s.version for s in bias_states.values()), default=1)
        
        markets = await self.db.read(
            _SELECT_MARKETS_STARTING_SOON,
            params={"now": now, "threshold": threshold},
            mappings=True,
        )
        
        if not markets:
            return 0
        
        self.logger.info(f"Capturing closing snapshots for {len(markets)} markets")
        
        snapshot_count = 0
        
        for market in markets:
            snapshots = await self._compute_market_snapshots(
                market_id=market["market_id"],
                snapshot_ts=now,
                bias_states=bias_states,
                bias_version=bias_version,
                is_closing=True,
            )
            
            if snapshots:
                await self._persist_snapshots(snapshots)
                snapshot_count += len(snapshots)
        
        return snapshot_count

    async def capture_late_closing_snapshots(self, limit: int = 50) -> int:
        """Capture closing snapshots for markets that already started but were missed.
        
        This is a recovery mechanism for events where we failed to capture
        a closing snapshot before the event started. Uses the most recent
        provider quote before event start as the "closing" line.
        
        Args:
            limit: Max number of markets to process per call
            
        Returns:
            Number of closing snapshots created
        """
        now = datetime.now(timezone.utc)
        # Look back up to 7 days for missed events
        min_time = now - timedelta(days=7)
        
        bias_states = await self._load_bias_states()
        bias_version = max((s.version for s in bias_states.values()), default=1)
        
        markets = await self.db.read(
            _SELECT_MARKETS_MISSED_CLOSING,
            params={"now": now, "min_time": min_time, "limit": limit},
            mappings=True,
        )
        
        if not markets:
            return 0
        
        self.logger.info({
            "late_closing_capture": {
                "markets_found": len(markets),
            }
        })
        
        snapshot_count = 0
        
        for market in markets:
            market_id = market["market_id"]
            start_time = market["start_time_utc"]
            
            # Use start_time as the snapshot timestamp (closing line)
            # This will use quotes from before the event started
            snapshots = await self._compute_market_snapshots(
                market_id=market_id,
                snapshot_ts=start_time,
                bias_states=bias_states,
                bias_version=bias_version,
                is_closing=True,
            )
            
            if snapshots:
                await self._persist_snapshots(snapshots)
                snapshot_count += len(snapshots)
                self.logger.debug({
                    "late_closing_captured": {
                        "market_id": market_id,
                        "event_id": market.get("event_id"),
                        "start_time": str(start_time),
                        "snapshots": len(snapshots),
                    }
                })
        
        if snapshot_count > 0:
            self.logger.info({
                "late_closing_complete": {
                    "snapshots_created": snapshot_count,
                }
            })
        
        return snapshot_count
    
    async def populate_ground_truth_closing(self) -> int:
        """Copy closing snapshots to ground_truth_closing table for scoring.
        
        This should be called after capture_closing_snapshots to ensure
        the ground_truth_closing table has the data needed for CLV scoring.
        
        Returns:
            Number of markets updated
        """
        # Find markets with closing snapshots but no ground_truth_closing entry
        markets = await self.db.read(
            _SELECT_MARKETS_NEEDING_CLOSING,
            mappings=True,
        )
        
        if not markets:
            return 0
        
        self.logger.info(f"Populating ground_truth_closing for {len(markets)} markets")
        
        count = 0
        for market in markets:
            try:
                await self.db.write(
                    _UPSERT_GROUND_TRUTH_CLOSING,
                    params={"market_id": market["market_id"]},
                )
                count += 1
            except Exception as e:
                self.logger.warning(f"Failed to upsert ground_truth_closing for market {market['market_id']}: {e}")
        
        return count
    
    async def _load_bias_states(self) -> Dict[BiasKey, BiasState]:
        """Load current bias estimates from database."""
        rows = await self.db.read(_SELECT_LATEST_BIAS, mappings=True)
        
        states = {}
        for row in rows:
            key = make_bias_key(
                row["sportsbook_id"],
                row["sport_id"],
                row["market_kind"],
            )
            states[key] = BiasState(
                sportsbook_id=row["sportsbook_id"],
                sport_id=row["sport_id"],
                market_kind=row["market_kind"],
                bias_factor=Decimal(str(row["bias_factor"])),
                variance=Decimal(str(row["variance"])),
                mse=Decimal("0"),
                sample_count=row["sample_count"],
                version=row["version"],
            )
        
        return states
    
    async def _compute_market_snapshots(
        self,
        market_id: int,
        snapshot_ts: datetime,
        bias_states: Dict[BiasKey, BiasState],
        bias_version: int,
        is_closing: bool,
    ) -> List[SnapshotResult]:
        """Compute consensus snapshots for all sides of a market."""
        # Get recent quotes (last 2 hours for freshness)
        since = snapshot_ts - timedelta(hours=2)
        
        quotes_raw = await self.db.read(
            _SELECT_MARKET_QUOTES,
            params={
                "market_id": market_id,
                "since": since,
                "until": snapshot_ts,
            },
            mappings=True,
        )
        
        if not quotes_raw:
            return []
        
        # Group by side, take latest quote per sportsbook per side
        quotes_by_side: Dict[str, Dict[int, BookQuote]] = {}
        
        for row in quotes_raw:
            side = row["side"]
            book_id = row["sportsbook_id"]
            
            if side not in quotes_by_side:
                quotes_by_side[side] = {}
            
            # Only keep the latest quote per book (query is ordered DESC)
            if book_id not in quotes_by_side[side]:
                quotes_by_side[side][book_id] = BookQuote(
                    sportsbook_id=book_id,
                    sport_id=row["sport_id"],
                    market_kind=row["market_kind"],
                    side=side,
                    prob=Decimal(str(row["imp_prob"])),
                    odds=Decimal(str(row["odds_eu"])),
                    timestamp=row["ts"],
                )
        
        results = []
        
        for side, book_quotes in quotes_by_side.items():
            quotes_list = list(book_quotes.values())
            
            consensus = self.consensus.compute_consensus(quotes_list, bias_states)
            
            if consensus is None:
                continue
            
            results.append(SnapshotResult(
                market_id=market_id,
                side=side,
                snapshot_ts=snapshot_ts,
                prob_consensus=consensus["prob_consensus"],
                odds_consensus=consensus["odds_consensus"],
                contributing_books=consensus["contributing_books"],
                std_dev=consensus["std_dev"],
                bias_version=bias_version,
                is_closing=is_closing,
            ))
        
        return results
    
    async def _persist_snapshots(self, snapshots: List[SnapshotResult]) -> None:
        """Persist snapshots to database."""
        touched: set[tuple[int, str]] = set()
        for snap in snapshots:
            await self.db.write(
                _UPSERT_SNAPSHOT,
                params={
                    "market_id": snap.market_id,
                    "side": snap.side,
                    "snapshot_ts": snap.snapshot_ts,
                    "prob_consensus": snap.prob_consensus,
                    "odds_consensus": snap.odds_consensus,
                    "contributing_books": snap.contributing_books,
                    "std_dev": snap.std_dev,
                    "bias_version": snap.bias_version,
                    "is_closing": snap.is_closing,
                },
            )
            touched.add((snap.market_id, snap.side))

        max_snapshots = int(self.params.ground_truth.max_snapshots_per_market)
        if max_snapshots <= 0:
            return
        for market_id, side in touched:
            await self.db.write(
                _DELETE_EXCESS_SNAPSHOTS,
                params={
                    "market_id": market_id,
                    "side": side,
                    "max_snapshots": max_snapshots,
                },
            )


# Matching functions for finding appropriate snapshots

_SELECT_MATCHED_SNAPSHOT = text("""
    SELECT 
        snapshot_ts, prob_consensus, odds_consensus,
        contributing_books, std_dev, bias_version, is_closing
    FROM ground_truth_snapshot
    WHERE market_id = :market_id
      AND side = :side
      AND snapshot_ts <= :submission_ts
    ORDER BY snapshot_ts DESC
    LIMIT 1
""")

_SELECT_CLOSING_SNAPSHOT = text("""
    SELECT 
        snapshot_ts, prob_consensus, odds_consensus,
        contributing_books, std_dev, bias_version
    FROM ground_truth_snapshot
    WHERE market_id = :market_id
      AND side = :side
      AND is_closing = true
    ORDER BY snapshot_ts DESC
    LIMIT 1
""")

_SELECT_BATCH_MATCHED_SNAPSHOTS = text("""
    WITH submission_data AS (
        SELECT 
            unnest(:submission_ids) as submission_id,
            unnest(:market_ids) as market_id,
            unnest(:sides) as side,
            unnest(:submission_times) as submission_ts
    )
    SELECT DISTINCT ON (sd.submission_id)
        sd.submission_id,
        gs.snapshot_ts,
        gs.prob_consensus,
        gs.odds_consensus,
        gs.contributing_books
    FROM submission_data sd
    LEFT JOIN ground_truth_snapshot gs ON 
        gs.market_id = sd.market_id
        AND gs.side = sd.side
        AND gs.snapshot_ts <= sd.submission_ts
    ORDER BY sd.submission_id, gs.snapshot_ts DESC
""")


@dataclass
class MatchedSnapshot:
    """Result of snapshot matching."""
    snapshot_ts: Optional[datetime]
    prob_consensus: Optional[Decimal]
    odds_consensus: Optional[Decimal]
    contributing_books: Optional[int]
    lag_minutes: Optional[int]  # Negative = snapshot before submission


async def find_matched_snapshot(
    db: Any,
    market_id: int,
    side: str,
    submission_ts: datetime,
    max_lag_hours: int = 12,
) -> Optional[MatchedSnapshot]:
    """Find the closest snapshot before a submission timestamp.
    
    Args:
        db: Database connection
        market_id: Market identifier
        side: Outcome side
        submission_ts: When the miner submitted
        max_lag_hours: Maximum hours between snapshot and submission
        
    Returns:
        MatchedSnapshot if found within tolerance, None otherwise
    """
    rows = await db.read(
        _SELECT_MATCHED_SNAPSHOT,
        params={
            "market_id": market_id,
            "side": side,
            "submission_ts": submission_ts,
        },
        mappings=True,
    )
    
    if not rows:
        return None
    
    row = rows[0]
    snapshot_ts = row["snapshot_ts"]
    
    # Check lag tolerance
    lag_seconds = (submission_ts - snapshot_ts).total_seconds()
    lag_minutes = int(lag_seconds / 60)
    
    if lag_seconds > max_lag_hours * 3600:
        return None  # Too old
    
    return MatchedSnapshot(
        snapshot_ts=snapshot_ts,
        prob_consensus=Decimal(str(row["prob_consensus"])),
        odds_consensus=Decimal(str(row["odds_consensus"])),
        contributing_books=row["contributing_books"],
        lag_minutes=-lag_minutes,  # Negative = snapshot before submission
    )


async def find_closing_snapshot(
    db: Any,
    market_id: int,
    side: str,
) -> Optional[MatchedSnapshot]:
    """Find the closing snapshot for a market.
    
    Args:
        db: Database connection
        market_id: Market identifier
        side: Outcome side
        
    Returns:
        MatchedSnapshot for closing line, None if not yet captured
    """
    rows = await db.read(
        _SELECT_CLOSING_SNAPSHOT,
        params={"market_id": market_id, "side": side},
        mappings=True,
    )
    
    if not rows:
        return None
    
    row = rows[0]
    return MatchedSnapshot(
        snapshot_ts=row["snapshot_ts"],
        prob_consensus=Decimal(str(row["prob_consensus"])),
        odds_consensus=Decimal(str(row["odds_consensus"])),
        contributing_books=row["contributing_books"],
        lag_minutes=None,
    )


async def find_matched_snapshots_batch(
    db: Any,
    submission_ids: List[int],
    market_ids: List[int],
    sides: List[str],
    submission_times: List[datetime],
) -> Dict[int, MatchedSnapshot]:
    """Find matched snapshots for a batch of submissions.
    
    Vectorized query for efficiency with large batches.
    
    Args:
        db: Database connection
        submission_ids: List of submission IDs
        market_ids: List of market IDs (parallel to submission_ids)
        sides: List of sides (parallel to submission_ids)
        submission_times: List of submission timestamps
        
    Returns:
        Dict mapping submission_id -> MatchedSnapshot
    """
    if not submission_ids:
        return {}
    
    # Convert datetimes to strings for the query
    ts_strings = [ts.isoformat() for ts in submission_times]
    
    rows = await db.read(
        _SELECT_BATCH_MATCHED_SNAPSHOTS,
        params={
            "submission_ids": submission_ids,
            "market_ids": market_ids,
            "sides": sides,
            "submission_times": ts_strings,
        },
        mappings=True,
    )
    
    results = {}
    for row in rows:
        sub_id = row["submission_id"]
        if row["snapshot_ts"] is not None:
            results[sub_id] = MatchedSnapshot(
                snapshot_ts=row["snapshot_ts"],
                prob_consensus=Decimal(str(row["prob_consensus"])) if row["prob_consensus"] else None,
                odds_consensus=Decimal(str(row["odds_consensus"])) if row["odds_consensus"] else None,
                contributing_books=row["contributing_books"],
                lag_minutes=None,  # Could compute if needed
            )
    
    return results


__all__ = [
    "SnapshotResult",
    "SnapshotPipeline",
    "MatchedSnapshot",
    "find_matched_snapshot",
    "find_closing_snapshot",
    "find_matched_snapshots_batch",
]


