"""Pydantic models for DataSyncer payloads.

These define the canonical interface contracts between the validator and the
dashboard Go API. Each model maps to one admin sync endpoint.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Miner Roster (POST /api/v1/admin/sync/roster)
# ---------------------------------------------------------------------------


class MinerRosterItem(BaseModel):
    """One miner in the roster. Maps to cloud PG `miner` table."""

    miner_id: int
    uid: int
    hotkey: str
    coldkey: str
    active: bool
    stake: float
    total_stake: float
    incentive: float
    emission: float
    trust: float
    rank: float


class RosterSyncPayload(BaseModel):
    """Full roster upsert, sent every scoring cycle."""

    validator_hotkey: str
    timestamp: datetime
    miners: list[MinerRosterItem]


# ---------------------------------------------------------------------------
# Rolling Scores (POST /api/v1/admin/sync/scores)
# ---------------------------------------------------------------------------


class MinerScoreRow(BaseModel):
    """One miner's rolling scores at a point in time.

    Source: validator ``miner_rolling_score`` table.
    Maps to cloud PG ``miner_score`` table (append-only time-series).
    """

    miner_id: int
    hotkey: str
    as_of: datetime

    # Final composite
    skill_score: Optional[float] = None
    weight: Optional[float] = None

    # Legacy dimension scores [0, 1] (pre-Cobb-Douglas)
    forecast_dim: Optional[float] = None
    skill_dim: Optional[float] = None
    econ_dim: Optional[float] = None
    info_dim: Optional[float] = None

    # Cobb-Douglas 5-pillar dimensions [0, 1]
    accuracy_dim: Optional[float] = None
    edge_dim: Optional[float] = None
    timeliness_dim: Optional[float] = None
    uniqueness_dim: Optional[float] = None
    marginal_dim: Optional[float] = None

    # Anti-sybil composite scores
    sos_crowd: Optional[float] = None
    sos_cluster: Optional[float] = None
    sos_composite: Optional[float] = None

    # Shapley contribution
    shapley_mean: Optional[float] = None
    cluster_id: Optional[int] = None
    cluster_size: Optional[int] = None

    # Normalized sub-scores [0, 1]
    fq_score: Optional[float] = None
    cal_score: Optional[float] = None
    sharp_score: Optional[float] = None
    edge_score: Optional[float] = None
    mes_score: Optional[float] = None
    sos_score: Optional[float] = None
    lead_score: Optional[float] = None

    # Raw metrics (before normalization)
    brier_mean: Optional[float] = None
    pss_mean: Optional[float] = None
    es_adj: Optional[float] = None
    es_mean: Optional[float] = None
    mes_mean: Optional[float] = None
    fq_raw: Optional[float] = None
    lead_ratio: Optional[float] = None

    # Sample sizes
    n_submissions: int = 0
    n_eff: Optional[float] = None


class ScoresSyncPayload(BaseModel):
    """Appends one time-series row per active miner, sent every scoring cycle."""

    validator_hotkey: str
    scoring_cycle_id: str
    timestamp: datetime
    scores: list[MinerScoreRow]


# ---------------------------------------------------------------------------
# Scored Submissions (POST /api/v1/admin/sync/submissions)
# ---------------------------------------------------------------------------


class ScoredSubmission(BaseModel):
    """One scored miner submission (settled markets only).

    Joins data from three validator tables:
      - miner_submission (submitted_at, priced_at, odds_eu, imp_prob, side)
      - submission_vs_close (cle, clv_prob, minutes_to_close)
      - submission_outcome_score (brier, logloss, pss, settled_at)
    """

    submission_id: int
    miner_id: int
    hotkey: str
    market_id: int
    side: str
    submitted_at: datetime
    priced_at: Optional[datetime] = None

    # Miner's odds
    odds_eu: float
    imp_prob: float

    # Odds scoring (vs closing line)
    cle: Optional[float] = None
    clv_prob: Optional[float] = None
    minutes_to_close: Optional[int] = None

    # Outcome scoring (vs realized outcome)
    brier: Optional[float] = None
    logloss: Optional[float] = None
    pss: Optional[float] = None
    pss_brier: Optional[float] = None
    pss_log: Optional[float] = None

    settled_at: Optional[datetime] = None


class SubmissionsSyncPayload(BaseModel):
    """Incremental scored submissions since last sync watermark."""

    validator_hotkey: str
    timestamp: datetime
    last_sync_id: int
    submissions: list[ScoredSubmission]


# ---------------------------------------------------------------------------
# Events / Markets / Outcomes (POST /api/v1/admin/sync/events)
# ---------------------------------------------------------------------------


class EventSync(BaseModel):
    """Denormalized event reference. Maps to cloud PG ``event`` table."""

    event_id: int
    league_code: str
    sport_code: str
    home_team: str
    away_team: str
    venue: Optional[str] = None
    start_time_utc: datetime
    status: str
    final_at: Optional[datetime] = None
    n_submissions: int = 0


class MarketSync(BaseModel):
    """Market reference. Maps to cloud PG ``market`` table."""

    market_id: int
    event_id: int
    kind: str
    line: Optional[float] = None


class OutcomeSync(BaseModel):
    """Settled outcome. Maps to cloud PG ``outcome`` table."""

    market_id: int
    result: Optional[str] = None
    score_home: Optional[float] = None
    score_away: Optional[float] = None
    settled_at: Optional[datetime] = None


class EventsSyncPayload(BaseModel):
    """Upsert events, markets, and outcomes (idempotent)."""

    validator_hotkey: str
    timestamp: datetime
    events: list[EventSync]
    markets: list[MarketSync]
    outcomes: list[OutcomeSync]


# ---------------------------------------------------------------------------
# Consensus Timeline (POST /api/v1/admin/sync/consensus)
# ---------------------------------------------------------------------------


class ConsensusRow(BaseModel):
    """Flat per-event consensus snapshot for the dashboard."""

    event_id: str
    timestamp: datetime
    moneyline_home: float
    moneyline_away: float
    spread_home: float
    total: float


class ConsensusSyncPayload(BaseModel):
    """Consensus timeline snapshots, sent every scoring cycle."""

    validator_hotkey: str
    timestamp: datetime
    payload: list[ConsensusRow]


# ---------------------------------------------------------------------------
# Pricing Feed (POST /api/v1/admin/sync/pricing)
# ---------------------------------------------------------------------------


class SubnetPerformanceRow(BaseModel):
    """Aggregate performance for one sport + market type combination."""

    sport_code: str
    market_kind: str
    n_scored: int = 0
    avg_cle: Optional[float] = None
    median_cle: Optional[float] = None
    avg_brier: Optional[float] = None
    avg_pss: Optional[float] = None
    top10_avg_cle: Optional[float] = None
    top10_median_cle: Optional[float] = None
    top10_avg_brier: Optional[float] = None
    top10_n_scored: int = 0


class SubnetPerformanceSyncPayload(BaseModel):
    """Aggregate subnet performance stats, sent every scoring cycle."""

    validator_hotkey: str
    timestamp: datetime
    total_scored: int = 0
    total_events: int = 0
    active_miners: int = 0
    rows: list[SubnetPerformanceRow]


class PricingConsensusRow(BaseModel):
    """Current consensus probability for one market/side."""

    market_id: int
    event_id: int
    kind: str  # MONEYLINE, SPREAD, TOTAL
    line: Optional[float] = None  # spread/total line
    side: str  # HOME, AWAY, OVER, UNDER
    prob_consensus: float
    odds_consensus: float
    contributing_books: int
    std_dev: Optional[float] = None
    snapshot_ts: datetime


class PricingBookQuote(BaseModel):
    """Individual sportsbook quote for one market/side."""

    market_id: int
    sportsbook: str
    side: str
    odds_eu: float
    imp_prob: float
    fetched_at: datetime


class PricingSyncPayload(BaseModel):
    """Full pricing snapshot for active markets. Sent every scoring cycle."""

    validator_hotkey: str
    timestamp: datetime
    consensus: list[PricingConsensusRow]
    book_quotes: list[PricingBookQuote]


# ---------------------------------------------------------------------------
# Heartbeat (POST /api/v1/admin/sync/heartbeat)
# ---------------------------------------------------------------------------


class ScoringHealth(BaseModel):
    last_cycle_timestamp: Optional[datetime] = None
    last_cycle_duration_seconds: Optional[float] = None
    last_cycle_submissions_scored: int = 0
    last_cycle_errors: int = 0
    jobs_status: dict[str, str] = Field(default_factory=dict)


class ProviderHealth(BaseModel):
    last_request_at: Optional[datetime] = None
    last_latency_seconds: Optional[float] = None
    errors_last_hour: int = 0
    status: str = "unknown"


class HeartbeatPayload(BaseModel):
    """Lightweight validator state, sent every ~60s."""

    validator_hotkey: str
    timestamp: datetime
    uptime_seconds: float
    current_step: int
    current_phase: str = "idle"

    # Fleet snapshot
    active_miners: int = 0
    total_miners: int = 0

    # Scoring health
    scoring: ScoringHealth = Field(default_factory=ScoringHealth)

    # Submission rates
    submissions_last_5m: int = 0
    submissions_last_15m: int = 0
    submissions_last_1h: int = 0

    # Data volume
    active_events: int = 0
    active_markets: int = 0

    # Provider health
    provider: ProviderHealth = Field(default_factory=ProviderHealth)

    # Resource usage
    memory_mb: Optional[float] = None
    db_pool_active: Optional[int] = None
    db_pool_size: Optional[int] = None
