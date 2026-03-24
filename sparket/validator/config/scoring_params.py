"""Scoring hyperparameters and configuration.

All scoring-related configuration lives here to ensure:
1. Single source of truth for hyperparameters
2. Determinism across validators (same params = same results)
3. Easy tuning and experimentation

IMPORTANT: Changes to these parameters affect validator consensus.
Coordinate parameter changes across the network.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, AliasChoices


class DecayParams(BaseModel):
    """Time decay parameters for rolling aggregates."""

    half_life_days: int = Field(
        default=10,
        ge=1,
        le=90,
        description="Half-life for exponential decay (days). After this many days, a submission's weight is halved.",
    )


class ShrinkageParams(BaseModel):
    """Bayesian shrinkage toward population mean."""

    k: Decimal = Field(
        default=Decimal("200"),
        ge=Decimal("1"),
        le=Decimal("1000"),
        description="Shrinkage factor. Higher = more shrinkage toward population mean for small samples.",
    )


class WindowParams(BaseModel):
    """Time window parameters for scoring."""

    rolling_window_days: int = Field(
        default=30,
        ge=7,
        le=180,
        description="Default rolling window for aggregate metrics (days).",
    )
    calibration_window_days: int = Field(
        default=60,
        ge=14,
        le=365,
        description="Window for calibration computation (days).",
    )
    min_samples_calibration: int = Field(
        default=100,
        ge=20,
        le=1000,
        description="Minimum samples required for calibration scoring.",
    )
    min_samples_originality: int = Field(
        default=50,
        ge=10,
        le=500,
        description="Minimum samples for originality/lead-lag scoring.",
    )


class DimensionWeights(BaseModel):
    """Weights for combining sub-scores into dimension scores."""

    # Forecast dimension: ForecastDim = w_fq * FQ + w_cal * CAL
    w_fq: Decimal = Field(
        default=Decimal("0.6"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for forecast quality in ForecastDim.",
    )
    w_cal: Decimal = Field(
        default=Decimal("0.4"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for calibration in ForecastDim.",
    )

    # Economic dimension: EconDim = w_edge * EDGE + w_mes * MES
    w_edge: Decimal = Field(
        default=Decimal("0.7"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for economic edge in EconDim.",
    )
    w_mes: Decimal = Field(
        default=Decimal("0.3"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for market efficiency in EconDim.",
    )

    # Info dimension: InfoDim = w_sos * SOS + w_lead * LEAD
    w_sos: Decimal = Field(
        default=Decimal("0.6"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for originality (SOS) in InfoDim.",
    )
    w_lead: Decimal = Field(
        default=Decimal("0.4"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for lead ratio in InfoDim.",
    )


class SkillScoreWeights(BaseModel):
    """Weights for combining dimensions into final SkillScore.
    
    4 dimensions:
    - Forecast: Accuracy vs outcome (Brier-based)
    - Skill: Relative skill vs market (PSS, time-adjusted)
    - Econ: Economic edge vs closing (CLE-based)
    - Info: Information advantage (SOS/Lead-lag)
    """

    # SkillScore = w_outcome_accuracy * ForecastDim
    #           + w_outcome_relative * SkillDim
    #           + w_odds_edge * EconDim
    #           + w_info_adv * InfoDim
    #
    # Defaults target an 80/20 split between odds-origination vs outcome skill.
    w_outcome_accuracy: Decimal = Field(
        default=Decimal("0.10"),
        ge=Decimal("0"),
        le=Decimal("1"),
        validation_alias=AliasChoices("w_forecast", "w_outcome_accuracy"),
        description="Weight for outcome accuracy (ForecastDim: FQ + CAL).",
    )
    w_outcome_relative: Decimal = Field(
        default=Decimal("0.10"),
        ge=Decimal("0"),
        le=Decimal("1"),
        validation_alias=AliasChoices("w_skill", "w_outcome_relative"),
        description="Weight for outcome relative skill (SkillDim: PSS).",
    )
    w_odds_edge: Decimal = Field(
        default=Decimal("0.50"),
        ge=Decimal("0"),
        le=Decimal("1"),
        validation_alias=AliasChoices("w_econ", "w_odds_edge"),
        description="Weight for odds origination edge (EconDim: ES + MES).",
    )
    w_info_adv: Decimal = Field(
        default=Decimal("0.30"),
        ge=Decimal("0"),
        le=Decimal("1"),
        validation_alias=AliasChoices("w_info", "w_info_adv"),
        description="Weight for information advantage (InfoDim: SOS + LEAD).",
    )

    # Backwards-compatible aliases for existing code/config
    @property
    def w_forecast(self) -> Decimal:
        return self.w_outcome_accuracy

    @property
    def w_skill(self) -> Decimal:
        return self.w_outcome_relative

    @property
    def w_econ(self) -> Decimal:
        return self.w_odds_edge

    @property
    def w_info(self) -> Decimal:
        return self.w_info_adv


class CalibrationParams(BaseModel):
    """Parameters for calibration scoring."""

    num_bins: int = Field(
        default=20,
        ge=5,
        le=100,
        description="Number of probability bins for calibration curve.",
    )
    min_samples_per_bin: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Minimum samples in a bin for it to contribute.",
    )


class SharpnessParams(BaseModel):
    """Parameters for sharpness scoring."""

    target_variance: Decimal = Field(
        default=Decimal("0.04"),
        ge=Decimal("0.01"),
        le=Decimal("0.25"),
        description="Target variance for 'sharp' predictions. Sharp = min(1, var/target_var).",
    )


class LeadLagParams(BaseModel):
    """Parameters for lead-lag analysis."""

    bucket_minutes: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Time bucket size for discretizing quotes (minutes).",
    )
    move_threshold: Decimal = Field(
        default=Decimal("0.02"),
        ge=Decimal("0.005"),
        le=Decimal("0.10"),
        description="Minimum probability change to count as a 'move'.",
    )
    lead_window_minutes: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Look-back window for detecting miner lead (minutes).",
    )
    lag_window_minutes: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Look-forward window for detecting miner lag (minutes).",
    )


class GroundTruthParams(BaseModel):
    """Parameters for ground truth (consensus) construction."""

    min_books_for_consensus: int = Field(
        default=2,
        ge=1,
        le=10,
        description="Minimum sportsbooks required for valid consensus.",
    )
    bias_learning_rate: Decimal = Field(
        default=Decimal("0.1"),
        ge=Decimal("0.01"),
        le=Decimal("0.5"),
        description="EMA alpha for updating sportsbook bias estimates.",
    )
    bias_min_samples: int = Field(
        default=50,
        ge=10,
        le=500,
        description="Minimum settled outcomes before trusting bias estimate.",
    )
    min_variance: Decimal = Field(
        default=Decimal("0.0001"),
        ge=Decimal("0.00001"),
        le=Decimal("0.01"),
        description="Floor for variance in inverse-variance weighting.",
    )
    max_bias_factor: Decimal = Field(
        default=Decimal("2.0"),
        ge=Decimal("1.5"),
        le=Decimal("5.0"),
        description="Maximum allowed bias factor (books with higher bias excluded).",
    )
    min_bias_factor: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0.1"),
        le=Decimal("0.9"),
        description="Minimum allowed bias factor (books with lower bias excluded).",
    )
    # Snapshot parameters for time-series ground truth
    snapshot_interval_hours: int = Field(
        default=6,
        ge=1,
        le=24,
        description="Hours between consensus snapshots for fair time-matched comparison.",
    )
    max_snapshots_per_market: int = Field(
        default=28,
        ge=7,
        le=56,
        description="Maximum snapshots to store per market (7 days × 4 per day = 28).",
    )
    snapshot_match_tolerance_hours: int = Field(
        default=12,
        ge=1,
        le=24,
        description="Max hours between submission and snapshot for valid match.",
    )


class NormalizationParams(BaseModel):
    """Parameters for score normalization."""

    method: Literal["zscore_logistic", "percentile", "minmax"] = Field(
        default="zscore_logistic",
        description="Normalization method for converting raw metrics to [0,1].",
    )
    logistic_alpha: Decimal = Field(
        default=Decimal("1.5"),
        ge=Decimal("0.5"),
        le=Decimal("5.0"),
        description="Steepness parameter for logistic normalization.",
    )
    min_count_for_zscore: int = Field(
        default=10,
        ge=2,
        le=1000,
        description="Minimum miner count to use z-score normalization.",
    )


class WorkerParams(BaseModel):
    """Scoring worker process parameters."""

    batch_interval_hours: int = Field(
        default=6,
        ge=1,
        le=24,
        description="Hours between heavy batch scoring jobs.",
    )
    ground_truth_interval_minutes: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Minutes between checking for closing snapshot updates.",
    )
    checkpoint_interval: int = Field(
        default=100,
        ge=10,
        le=1000,
        description="Number of items between job checkpoints.",
    )
    heartbeat_interval_sec: int = Field(
        default=10,
        ge=5,
        le=60,
        description="Seconds between worker heartbeat updates.",
    )
    heartbeat_timeout_sec: int = Field(
        default=60,
        ge=30,
        le=300,
        description="Seconds before considering a worker dead.",
    )
    max_restart_attempts: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Maximum worker restart attempts before giving up.",
    )
    restart_delay_sec: int = Field(
        default=5,
        ge=1,
        le=60,
        description="Seconds to wait before restarting a failed worker.",
    )


class TimeWeightParams(BaseModel):
    """Time-to-close weighting parameters.

    Applied as SCORE MULTIPLIER (not weight multiplier) with asymmetric treatment:
    - Early GOOD predictions: Full credit (rewarded for skill)
    - Early BAD predictions: Clipped penalty (expected uncertainty)
    - Late GOOD predictions: Reduced credit (might be copying)
    - Late BAD predictions: Full penalty (no excuse)
    """

    min_minutes: int = Field(
        default=60,
        ge=5,
        le=360,
        description="Submissions below this get floor factor. Default 1 hour.",
    )
    max_minutes: int = Field(
        default=10080,
        ge=1440,
        le=20160,
        description="Submissions beyond this get factor 1.0. Default 7 days.",
    )
    floor_factor: Decimal = Field(
        default=Decimal("0.1"),
        ge=Decimal("0.01"),
        le=Decimal("0.5"),
        description="Minimum factor for very late submissions (good predictions).",
    )
    early_penalty_clip: Decimal = Field(
        default=Decimal("0.7"),
        ge=Decimal("0.3"),
        le=Decimal("0.9"),
        description="Penalty multiplier for early bad predictions (forgive uncertainty).",
    )
    enabled: bool = Field(
        default=True,
        description="Whether to apply time-to-close weighting.",
    )


class CobbDouglasParams(BaseModel):
    """Cobb-Douglas multiplicative scoring exponents and thresholds."""

    accuracy_exponent: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0.1"),
        le=Decimal("3.0"),
        description="Exponent for Accuracy pillar (compressed — soft prerequisite).",
    )
    edge_exponent: Decimal = Field(
        default=Decimal("1.0"),
        ge=Decimal("0.1"),
        le=Decimal("3.0"),
        description="Exponent for Edge pillar (linear — core sharpness).",
    )
    timeliness_exponent: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0.1"),
        le=Decimal("3.0"),
        description="Exponent for Timeliness pillar (compressed — soft prerequisite).",
    )
    uniqueness_exponent: Decimal = Field(
        default=Decimal("1.5"),
        ge=Decimal("0.1"),
        le=Decimal("3.0"),
        description="Exponent for Uniqueness pillar (amplified — strongest discriminator).",
    )
    marginal_exponent: Decimal = Field(
        default=Decimal("1.0"),
        ge=Decimal("0.1"),
        le=Decimal("3.0"),
        description="Exponent for Marginal contribution pillar (linear — validates uniqueness).",
    )
    floor_threshold: Decimal = Field(
        default=Decimal("0.30"),
        ge=Decimal("0.15"),
        le=Decimal("0.50"),
        description="Brier score hard floor. Miners above this get zero weight.",
    )
    epsilon: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0.001"),
        le=Decimal("0.1"),
        description="Minimum dimension value before exponentiation (prevents log(0)).",
    )


class CompositeSOSParams(BaseModel):
    """Parameters for composite SOS (Uniqueness dimension)."""

    w_market: Decimal = Field(
        default=Decimal("0.2"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for SOS_market (independence from market consensus).",
    )
    w_crowd: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for SOS_crowd (independence from other miners).",
    )
    w_cluster: Decimal = Field(
        default=Decimal("0.3"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for SOS_cluster (spectral clustering penalty).",
    )
    correlation_window_days: int = Field(
        default=30,
        ge=7,
        le=90,
        description="Window for computing pairwise miner correlations.",
    )
    min_common_markets: int = Field(
        default=10,
        ge=3,
        le=100,
        description="Minimum common markets for a valid correlation estimate.",
    )
    cluster_correlation_threshold: Decimal = Field(
        default=Decimal("0.85"),
        ge=Decimal("0.5"),
        le=Decimal("0.99"),
        description="Pairwise correlation above which miners are considered clustered.",
    )
    crowd_max_corr_weight: Decimal = Field(
        default=Decimal("0.6"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description="Weight for max pairwise correlation in SOS_crowd. Prevents dilution in large networks.",
    )
    cluster_soft_floor: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0.2"),
        le=Decimal("0.8"),
        description="Correlation below which no soft cluster penalty is applied.",
    )
    cluster_soft_ceil: Decimal = Field(
        default=Decimal("0.95"),
        ge=Decimal("0.8"),
        le=Decimal("1.0"),
        description="Correlation at or above which soft cluster penalty reaches 1.0.",
    )
    low_overlap_threshold: Decimal = Field(
        default=Decimal("0.15"),
        ge=Decimal("0"),
        le=Decimal("0.5"),
        description="Baseline unscoreable peer fraction before overlap penalty kicks in.",
    )
    low_overlap_max_unscoreable: Decimal = Field(
        default=Decimal("0.5"),
        ge=Decimal("0.2"),
        le=Decimal("1.0"),
        description="Unscoreable peer fraction at which overlap penalty reaches 1.0.",
    )
    enable_sub_window_correlation: bool = Field(
        default=True,
        description="Enable sub-window correlation to detect rotation attacks.",
    )


class ShapleyParams(BaseModel):
    """Parameters for Monte Carlo Shapley contribution scoring."""

    k_permutations: int = Field(
        default=500,
        ge=50,
        le=5000,
        description="Number of random permutations for Shapley estimation.",
    )
    seed_prefix: int = Field(
        default=42,
        description="Seed prefix for deterministic RNG (combined with epoch + market_id).",
    )
    min_miners_for_shapley: int = Field(
        default=5,
        ge=2,
        le=50,
        description="Minimum active miners required to compute Shapley values.",
    )
    max_parallel_markets: int = Field(
        default=4,
        ge=1,
        le=16,
        description="Number of settled markets to process in parallel.",
    )
    batch_interval_minutes: int = Field(
        default=60,
        ge=15,
        le=1440,
        description="How often to run Shapley computation (minutes).",
    )
    prob_clamp_epsilon: Decimal = Field(
        default=Decimal("0.005"),
        ge=Decimal("0.001"),
        le=Decimal("0.05"),
        description="Clamp probabilities to [ε, 1-ε] before logit transform.",
    )


class SecurityBounds(BaseModel):
    """Security bounds for input validation and exploit prevention."""

    odds_min: Decimal = Field(
        default=Decimal("1.01"),
        ge=Decimal("1.001"),
        le=Decimal("1.1"),
        description="Minimum valid decimal odds (exclusive lower bound).",
    )
    odds_max: Decimal = Field(
        default=Decimal("1000"),
        ge=Decimal("100"),
        le=Decimal("10000"),
        description="Maximum valid decimal odds (inclusive upper bound).",
    )
    prob_min: Decimal = Field(
        default=Decimal("0.001"),
        ge=Decimal("0.0001"),
        le=Decimal("0.01"),
        description="Minimum valid probability.",
    )
    prob_max: Decimal = Field(
        default=Decimal("0.999"),
        ge=Decimal("0.99"),
        le=Decimal("0.9999"),
        description="Maximum valid probability.",
    )
    cle_min: Decimal = Field(
        default=Decimal("-1"),
        ge=Decimal("-2"),
        le=Decimal("-0.5"),
        description="Minimum CLE (worst possible edge).",
    )
    cle_max: Decimal = Field(
        default=Decimal("10"),
        ge=Decimal("1"),
        le=Decimal("100"),
        description="Maximum CLE (prevents gaming with extreme odds).",
    )
    prob_sum_tolerance: Decimal = Field(
        default=Decimal("0.01"),
        ge=Decimal("0.001"),
        le=Decimal("0.1"),
        description="Tolerance for probability vector sum deviation from 1.0.",
    )


class IngestParams(BaseModel):
    """Parameters for ingest validation and rate limits."""

    odds_window_days: int = Field(
        default=7,
        ge=1,
        le=30,
        description="Days ahead for accepting odds submissions.",
    )
    odds_bucket_seconds: int = Field(
        default=60,
        ge=10,
        le=3600,
        description="Time bucket size for odds deduplication.",
    )
    priced_at_tolerance_sec: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Allowed priced_at drift from received_at (seconds).",
    )
    max_submissions_per_market_day: int = Field(
        default=200,
        ge=10,
        le=5000,
        description="Per-miner daily cap per market to prevent farming.",
    )
    outcome_window_hours: int = Field(
        default=12,
        ge=1,
        le=72,
        description="Hours after start time to accept outcome submissions.",
    )
    max_outcomes_per_event_day: int = Field(
        default=50,
        ge=5,
        le=1000,
        description="Per-miner daily cap per event for outcomes.",
    )
    outcome_bucket_seconds: int = Field(
        default=300,
        ge=30,
        le=3600,
        description="Time bucket size for outcome deduplication.",
    )
    outcome_max_retries: int = Field(
        default=5,
        ge=0,
        le=50,
        description="Maximum inbox retries before marking processed.",
    )


class RetentionParams(BaseModel):
    """Retention windows in days for cleanup jobs."""

    provider_quote_days: int = Field(default=30, ge=1, le=365)
    provider_closing_days: int = Field(default=30, ge=1, le=365)
    miner_submission_days: int = Field(default=30, ge=1, le=365)
    submission_vs_close_days: int = Field(default=30, ge=1, le=365)
    submission_outcome_score_days: int = Field(default=30, ge=1, le=365)
    ground_truth_snapshot_days: int = Field(default=30, ge=1, le=365)
    ground_truth_closing_days: int = Field(default=30, ge=1, le=365)
    inbox_processed_days: int = Field(default=7, ge=1, le=365)
    outbox_sent_days: int = Field(default=7, ge=1, le=365)
    scoring_job_state_days: int = Field(default=30, ge=1, le=365)
    scoring_worker_heartbeat_days: int = Field(default=7, ge=1, le=365)
    scoring_work_queue_days: int = Field(default=7, ge=1, le=365)


class WeightEmissionParams(BaseModel):
    """Parameters for weight emission and burn allocation.

    Controls how miner rewards are distributed between actual miners
    and the subnet owner (burn hotkey).
    """

    burn_rate: Decimal = Field(
        default=Decimal("0.9"),
        ge=Decimal("0"),
        le=Decimal("1"),
        description=(
            "Fraction of total weight allocated to the subnet owner (burn hotkey). "
            "A burn_rate of 0.9 means 90% of emissions go to the burn hotkey, "
            "and the remaining 10% is distributed proportionally among miners. "
            "Set to 0 to disable burn allocation."
        ),
    )


class ScoringParams(BaseModel):
    """Master configuration for all scoring parameters."""

    decay: DecayParams = Field(default_factory=DecayParams)
    shrinkage: ShrinkageParams = Field(default_factory=ShrinkageParams)
    windows: WindowParams = Field(default_factory=WindowParams)
    dimension_weights: DimensionWeights = Field(default_factory=DimensionWeights)
    skill_score_weights: SkillScoreWeights = Field(default_factory=SkillScoreWeights)
    calibration: CalibrationParams = Field(default_factory=CalibrationParams)
    sharpness: SharpnessParams = Field(default_factory=SharpnessParams)
    lead_lag: LeadLagParams = Field(default_factory=LeadLagParams)
    ground_truth: GroundTruthParams = Field(default_factory=GroundTruthParams)
    normalization: NormalizationParams = Field(default_factory=NormalizationParams)
    worker: WorkerParams = Field(default_factory=WorkerParams)
    security: SecurityBounds = Field(default_factory=SecurityBounds)
    time_weight: TimeWeightParams = Field(default_factory=TimeWeightParams)
    ingest: IngestParams = Field(default_factory=IngestParams)
    retention: RetentionParams = Field(default_factory=RetentionParams)
    weight_emission: WeightEmissionParams = Field(default_factory=WeightEmissionParams)
    cobb_douglas: CobbDouglasParams = Field(default_factory=CobbDouglasParams)
    composite_sos: CompositeSOSParams = Field(default_factory=CompositeSOSParams)
    shapley: ShapleyParams = Field(default_factory=ShapleyParams)


# Default instance for easy import
DEFAULT_SCORING_PARAMS = ScoringParams()


def get_scoring_params() -> ScoringParams:
    """Get scoring parameters. Future: load from config file or chain."""
    return DEFAULT_SCORING_PARAMS


__all__ = [
    "DecayParams",
    "ShrinkageParams",
    "WindowParams",
    "DimensionWeights",
    "SkillScoreWeights",
    "CalibrationParams",
    "SharpnessParams",
    "LeadLagParams",
    "GroundTruthParams",
    "NormalizationParams",
    "WorkerParams",
    "TimeWeightParams",
    "SecurityBounds",
    "IngestParams",
    "RetentionParams",
    "WeightEmissionParams",
    "CobbDouglasParams",
    "CompositeSOSParams",
    "ShapleyParams",
    "ScoringParams",
    "DEFAULT_SCORING_PARAMS",
    "get_scoring_params",
]

