"""Pydantic models for the scoring ledger checkpoint+delta system.

Two export types:
- CheckpointWindow: full accumulator state per miner (published every scoring cycle)
- DeltaWindow: settled submission outcome scores for independent Brier verification
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Schema version - bump on breaking changes to ledger format
# ---------------------------------------------------------------------------

LEDGER_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Recompute record (embedded in checkpoint manifest on epoch bumps)
# ---------------------------------------------------------------------------


class RecomputeReasonCode(str, Enum):
    """Standardized reason codes for epoch bumps."""

    SDIO_FEED_ERROR = "SDIO_FEED_ERROR"
    SDIO_OUTAGE = "SDIO_OUTAGE"
    SCORING_BUG = "SCORING_BUG"
    DB_CORRUPTION = "DB_CORRUPTION"
    DB_MIGRATION = "DB_MIGRATION"
    CONFIG_CHANGE = "CONFIG_CHANGE"
    MANUAL_CORRECTION = "MANUAL_CORRECTION"
    SCHEDULED_RECALIBRATION = "SCHEDULED_RECALIBRATION"


class RecomputeRecord(BaseModel):
    """Structured record of an epoch bump / recompute event."""

    epoch: int
    previous_epoch: int
    reason_code: RecomputeReasonCode
    reason_detail: str = Field(min_length=1)
    affected_event_ids: list[int] = Field(default_factory=list)
    severity: str = Field(pattern=r"^(correction|bugfix|recovery)$")
    timestamp: datetime
    code_version: str = Field(min_length=1)


# ---------------------------------------------------------------------------
# Manifest (signed header for both checkpoint and delta)
# ---------------------------------------------------------------------------


class LedgerManifest(BaseModel):
    """Signed manifest header for a ledger window."""

    schema_version: int = LEDGER_SCHEMA_VERSION
    window_type: str = Field(pattern=r"^(checkpoint|delta)$")
    window_start: datetime
    window_end: datetime
    checkpoint_epoch: int
    content_hashes: dict[str, str] = Field(
        description="Map of section name -> SHA256 hex digest"
    )
    primary_hotkey: str
    signature: str = ""
    created_at: datetime
    recompute_record: RecomputeRecord | None = None


# ---------------------------------------------------------------------------
# Checkpoint contents
# ---------------------------------------------------------------------------


class MetricAccumulator(BaseModel):
    """Weighted sum / weight sum pair for a single metric."""

    ws: float = Field(description="weighted_sum = sum(value_i * decay_weight_i)")
    wt: float = Field(description="weight_sum = sum(decay_weight_i)")


class AccumulatorEntry(BaseModel):
    """Per-miner accumulator state in a checkpoint.

    Contains (ws, wt) pairs for each metric plus derived means.
    Auditors can verify: derived_mean == ws / wt (or fallback if wt == 0).
    """

    miner_id: int
    hotkey: str
    uid: int
    n_submissions: int = 0
    n_outcomes: int = 0

    # Per-metric accumulators
    brier: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    fq: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    pss: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    es: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    mes: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    sos: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))
    lead: MetricAccumulator = Field(default_factory=lambda: MetricAccumulator(ws=0, wt=0))

    # Derived means (convenience, auditors verify these match ws/wt)
    brier_mean: float = 0.0
    fq_raw: float = 0.0
    pss_mean: float = 0.0
    es_adj: float = 0.0
    mes_mean: float = 0.0
    sos_score: float = 0.5
    lead_score: float = 0.5
    cal_score: float = 0.5
    sharp_score: float = 0.5

    def derive_means(self) -> None:
        """Compute derived means from accumulator pairs.

        Uses the same fallback defaults as SkillScoreJob._to_float_safe().
        """
        self.brier_mean = self.brier.ws / self.brier.wt if self.brier.wt else 0.0
        self.fq_raw = self.fq.ws / self.fq.wt if self.fq.wt else 0.0
        self.pss_mean = self.pss.ws / self.pss.wt if self.pss.wt else 0.0
        self.es_adj = self.es.ws / self.es.wt if self.es.wt else 0.0
        self.mes_mean = self.mes.ws / self.mes.wt if self.mes.wt else 0.5
        self.sos_score = self.sos.ws / self.sos.wt if self.sos.wt else 0.5
        self.lead_score = self.lead.ws / self.lead.wt if self.lead.wt else 0.5


class MinerRosterEntry(BaseModel):
    """Miner metadata in a checkpoint."""

    miner_id: int
    uid: int
    hotkey: str
    active: bool


class ScoringConfigSnapshot(BaseModel):
    """Serialized scoring parameters for reproducibility."""

    params: dict[str, Any] = Field(default_factory=dict)


class ChainParamsSnapshot(BaseModel):
    """Chain parameters used for weight computation."""

    burn_rate: float
    burn_uid: int | None = None
    max_weight_limit: float
    min_allowed_weights: int
    n_neurons: int


class CheckpointWindow(BaseModel):
    """Full accumulator state snapshot."""

    manifest: LedgerManifest
    roster: list[MinerRosterEntry] = Field(default_factory=list)
    accumulators: list[AccumulatorEntry] = Field(default_factory=list)
    scoring_config: ScoringConfigSnapshot = Field(default_factory=ScoringConfigSnapshot)
    chain_params: ChainParamsSnapshot | None = None


# ---------------------------------------------------------------------------
# Delta contents
# ---------------------------------------------------------------------------


class SettledSubmissionEntry(BaseModel):
    """Per-submission outcome score in a delta (settled markets only)."""

    miner_id: int
    market_id: int
    side: str
    imp_prob: float
    brier: float | None = None
    pss: float | None = None
    settled_at: datetime


class OutcomeEntry(BaseModel):
    """Public outcome for a settled market."""

    market_id: int
    event_id: int
    result: str | None = None
    score_home: float | None = None
    score_away: float | None = None
    settled_at: datetime


class DeltaWindow(BaseModel):
    """Settled submission scores since the previous window."""

    manifest: LedgerManifest
    settled_submissions: list[SettledSubmissionEntry] = Field(default_factory=list)
    settled_outcomes: list[OutcomeEntry] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# MinerMetrics: the input to compute_weights (shared by primary + auditor)
# ---------------------------------------------------------------------------


class MinerMetrics(BaseModel):
    """Derived rolling means - the input to normalization.

    On the primary: read directly from miner_rolling_score DB columns.
    On the auditor: derived from AccumulatorEntry (mean = ws / wt).
    Both paths must produce identical values.
    """

    uid: int
    hotkey: str
    fq_raw: float = 0.0
    pss_mean: float = 0.0
    es_adj: float = 0.0
    mes_mean: float = 0.5
    cal_score: float = 0.5
    sharp_score: float = 0.5
    sos_score: float = 0.5
    lead_score: float = 0.5
    brier_mean: float = 0.0
    # Cobb-Douglas new dimensions (None = not yet computed, use bootstrap defaults)
    uniqueness_dim: float | None = None
    marginal_dim: float | None = None

    @classmethod
    def from_accumulator(cls, acc: AccumulatorEntry) -> MinerMetrics:
        """Build MinerMetrics from an AccumulatorEntry (auditor path)."""
        acc.derive_means()
        return cls(
            uid=acc.uid,
            hotkey=acc.hotkey,
            fq_raw=acc.fq_raw,
            pss_mean=acc.pss_mean,
            es_adj=acc.es_adj,
            mes_mean=acc.mes_mean,
            cal_score=acc.cal_score,
            sharp_score=acc.sharp_score,
            sos_score=acc.sos_score,
            lead_score=acc.lead_score,
            brier_mean=acc.brier_mean,
        )


__all__ = [
    "LEDGER_SCHEMA_VERSION",
    "AccumulatorEntry",
    "ChainParamsSnapshot",
    "CheckpointWindow",
    "DeltaWindow",
    "LedgerManifest",
    "MetricAccumulator",
    "MinerMetrics",
    "MinerRosterEntry",
    "OutcomeEntry",
    "RecomputeReasonCode",
    "RecomputeRecord",
    "ScoringConfigSnapshot",
    "SettledSubmissionEntry",
]
