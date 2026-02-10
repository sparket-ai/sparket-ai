"""Miner submissions and derived scoring tables."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, price_side_enum


class MinerSubmission(Base):
    __tablename__ = "miner_submission"

    submission_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
        comment="Unique identifier for each miner-submitted odds snapshot",
    )
    miner_id: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        comment="Submitting miner (reference to reference.miner)",
    )
    miner_hotkey: Mapped[str] = mapped_column(
        String,
        nullable=False,
        comment="Miner hotkey to disambiguate lifecycle with same uid",
    )
    market_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("market.market_id", ondelete="CASCADE"),
        nullable=False,
        comment="Market the submission applies to",
    )
    side: Mapped[str] = mapped_column(
        price_side_enum,
        nullable=False,
        comment="Which outcome within the market this odds entry refers to",
    )
    submitted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Timestamp when the validator received the submission (UTC)",
    )
    priced_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="Optional: miner-declared timestamp when they priced the market (UTC)",
    )
    odds_eu: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Miner-supplied decimal odds",
    )
    imp_prob: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Instant implied probability (1/odds_eu)",
    )
    payload: Mapped[dict] = mapped_column(
        JSONB,
        default=dict,
        comment="Opaque metadata from the miner (model version, features, etc.)",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["miner_id", "miner_hotkey"],
            ["miner.miner_id", "miner.hotkey"],
            name="fk_miner_submission_miner",
            ondelete="CASCADE",
        ),
        Index(
            "uq_miner_submission",
            "miner_id",
            "miner_hotkey",
            "market_id",
            "side",
            "submitted_at",
            unique=True,
        ),
        Index("ix_miner_submission_market_ts", "market_id", "submitted_at"),
        Index("ix_miner_submission_miner_ts", "miner_id", "miner_hotkey", "submitted_at"),
    )


class SubmissionVsClose(Base):
    """Comparison of miner submission against ground truth.

    Contains TWO comparisons:
    1. vs CLOSING LINE (close_*): For CLV/CLE economic edge calculations
    2. vs MATCHED SNAPSHOT (snapshot_*): For fair PSS skill comparison at submission time
    """

    __tablename__ = "submission_vs_close"

    submission_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("miner_submission.submission_id", ondelete="CASCADE"),
        primary_key=True,
        comment="Links back to the miner submission being scored",
    )
    provider_basis: Mapped[str] = mapped_column(
        nullable=False,
        comment="Which provider or basket was used to determine the closing reference",
    )

    # --- CLOSING LINE COMPARISON (for CLV/CLE) ---
    close_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="Timestamp of the closing quote (UTC)",
    )
    close_odds_eu: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Closing decimal odds for the same side",
    )
    close_imp_prob: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Closing implied probability (raw)",
    )
    close_imp_prob_norm: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Closing implied probability after overround normalization",
    )
    clv_odds: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Odds-based closing line value ( (O_miner - O_close) / O_close )",
    )
    clv_prob: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Probability-space edge ( (p_close - p_miner) / p_close )",
    )
    cle: Mapped[float] = mapped_column(
        Numeric,
        nullable=False,
        comment="Closed-line efficiency (expected value: O_miner * p_close - 1)",
    )
    minutes_to_close: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="How many minutes prior to event start the submission arrived",
    )

    # --- MATCHED SNAPSHOT COMPARISON (for fair PSS) ---
    snapshot_ts: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="Timestamp of the matched snapshot (closest to submission time)",
    )
    snapshot_prob: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Consensus probability from matched snapshot",
    )
    snapshot_odds: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Consensus odds from matched snapshot",
    )
    snapshot_lag_minutes: Mapped[int | None] = mapped_column(
        Integer,
        comment="Minutes between submission and matched snapshot (negative = snapshot before)",
    )

    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=func.now,
        nullable=False,
        comment="When this delta was computed (UTC)",
    )
    ground_truth_version: Mapped[int | None] = mapped_column(
        Integer,
        comment="Version of ground truth (sportsbook bias) used for this computation",
    )

    __table_args__ = (
        Index("ix_submission_vs_close_minutes", "minutes_to_close"),
        Index("ix_submission_vs_close_clv", "clv_prob"),
        Index("ix_submission_vs_close_cle", "cle"),
        Index("ix_submission_vs_close_snapshot", "snapshot_ts"),
    )


class MinerMarketStats(Base):
    __tablename__ = "miner_market_stats"

    miner_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Miner being evaluated",
    )
    miner_hotkey: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        comment="Miner hotkey at the time of observation",
    )
    market_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("market.market_id", ondelete="CASCADE"),
        primary_key=True,
        comment="Market context",
    )
    window_start: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        comment="Beginning of the observation window (UTC)",
    )
    window_end: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        comment="End of the observation window (UTC)",
    )
    corr_raw: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Correlation between miner implied probabilities and provider",
    )
    corr_norm: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Correlation using normalized probabilities",
    )
    lead_seconds: Mapped[int | None] = mapped_column(
        Integer,
        comment="Average seconds by which miner leads provider moves",
    )
    moves_followed_ratio: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Fraction of provider moves the miner followed within the window",
    )
    moves_led_ratio: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Fraction of moves the miner led",
    )
    n_obs: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Number of quote observations considered",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["miner_id", "miner_hotkey"],
            ["miner.miner_id", "miner.hotkey"],
            name="fk_miner_market_stats_miner",
            ondelete="CASCADE",
        ),
        Index("ix_miner_market_stats_window_end", "window_end"),
    )


class SubmissionOutcomeScore(Base):
    __tablename__ = "submission_outcome_score"

    submission_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("miner_submission.submission_id", ondelete="CASCADE"),
        primary_key=True,
        comment="Miner submission being evaluated for outcome-calibrated metrics",
    )
    # Miner scores
    brier: Mapped[float | None] = mapped_column(Numeric, comment="Miner Brier score for this market")
    logloss: Mapped[float | None] = mapped_column(Numeric, comment="Miner log loss")
    # Ground truth (provider/consensus) scores
    provider_brier: Mapped[float | None] = mapped_column(Numeric, comment="Reference provider Brier score")
    provider_logloss: Mapped[float | None] = mapped_column(Numeric, comment="Reference provider log loss")
    # Probability Skill Scores (relative improvement)
    pss: Mapped[float | None] = mapped_column(Numeric, comment="Predictive skill score (1 - Brier_miner / Brier_provider)")
    pss_brier: Mapped[float | None] = mapped_column(Numeric, comment="PSS using Brier score")
    pss_log: Mapped[float | None] = mapped_column(Numeric, comment="PSS using log loss")
    # Outcome metadata
    outcome_vector: Mapped[dict | None] = mapped_column(
        JSONB,
        comment="One-hot outcome vector for audit (e.g., [0,1,0])",
    )
    settled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        comment="When the underlying outcome settled (UTC)",
    )


class MinerRollingScore(Base):
    __tablename__ = "miner_rolling_score"

    miner_id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        comment="Miner identifier",
    )
    miner_hotkey: Mapped[str] = mapped_column(
        String,
        primary_key=True,
        comment="Miner hotkey at the time of aggregation",
    )
    as_of: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        primary_key=True,
        comment="Timestamp the rolling view represents (UTC)",
    )
    window_days: Mapped[int] = mapped_column(
        Integer,
        primary_key=True,
        comment="Number of days in the rolling window",
    )
    n_submissions: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        comment="Number of submissions contributing to the window",
    )
    # Effective sample size after time decay
    n_eff: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Effective sample size after exponential decay weighting",
    )
    # Raw aggregate metrics (before normalization)
    es_mean: Mapped[float | None] = mapped_column(Numeric, comment="Average closed-line efficiency (CLE)")
    es_std: Mapped[float | None] = mapped_column(Numeric, comment="Std dev of CLE for risk adjustment")
    es_adj: Mapped[float | None] = mapped_column(Numeric, comment="Risk-adjusted edge (es_mean / es_std)")
    mes_mean: Mapped[float | None] = mapped_column(Numeric, comment="Average market efficiency score")
    sos_mean: Mapped[float | None] = mapped_column(Numeric, comment="Average originality score (1 - |corr|)")
    pss_mean: Mapped[float | None] = mapped_column(Numeric, comment="Average predictive skill score (time-adjusted)")
    fq_raw: Mapped[float | None] = mapped_column(Numeric, comment="Forecast quality raw score")
    brier_mean: Mapped[float | None] = mapped_column(Numeric, comment="Average Brier score vs outcome (raw accuracy)")
    lead_ratio: Mapped[float | None] = mapped_column(Numeric, comment="Fraction of moves where miner led market")
    # Normalized per-dimension scores [0, 1]
    fq_score: Mapped[float | None] = mapped_column(Numeric, comment="Forecast quality normalized [0,1]")
    cal_score: Mapped[float | None] = mapped_column(Numeric, comment="Calibration score [0,1]")
    sharp_score: Mapped[float | None] = mapped_column(Numeric, comment="Sharpness score [0,1]")
    edge_score: Mapped[float | None] = mapped_column(Numeric, comment="Economic edge normalized [0,1]")
    mes_score: Mapped[float | None] = mapped_column(Numeric, comment="Market efficiency normalized [0,1]")
    sos_score: Mapped[float | None] = mapped_column(Numeric, comment="Originality score normalized [0,1]")
    lead_score: Mapped[float | None] = mapped_column(Numeric, comment="Lead ratio normalized [0,1]")
    # Macro dimensions (composite of sub-scores)
    forecast_dim: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Forecast dimension = 0.6*FQ + 0.4*CAL",
    )
    econ_dim: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Economic dimension = 0.7*EDGE + 0.3*MES",
    )
    info_dim: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Info dimension = 0.6*SOS + 0.4*LEAD",
    )
    skill_dim: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Skill dimension = PSS_norm (outcome relative skill)",
    )
    # Final composite score
    skill_score: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Final skill score = 0.10*ForecastDim + 0.10*SkillDim + 0.50*EconDim + 0.30*InfoDim",
    )
    composite_score: Mapped[float | None] = mapped_column(
        Numeric,
        comment="Legacy composite (deprecated, use skill_score)",
    )
    # Accumulator pairs for ledger export (ws = weighted_sum, wt = weight_sum)
    brier_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for Brier scores")
    brier_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for Brier scores")
    fq_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for FQ")
    fq_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for FQ")
    pss_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for PSS")
    pss_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for PSS")
    es_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for CLE")
    es_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for CLE")
    mes_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for MES")
    mes_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for MES")
    sos_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for SOS")
    sos_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for SOS")
    lead_ws: Mapped[float | None] = mapped_column(Numeric, comment="Weighted sum for lead ratio")
    lead_wt: Mapped[float | None] = mapped_column(Numeric, comment="Weight sum for lead ratio")
    # Versioning for consensus
    score_version: Mapped[int] = mapped_column(
        Integer,
        default=1,
        nullable=False,
        comment="Version number for cross-validator consensus",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["miner_id", "miner_hotkey"],
            ["miner.miner_id", "miner.hotkey"],
            name="fk_miner_rolling_score_miner",
            ondelete="CASCADE",
        ),
        Index("ix_miner_rolling_score_as_of", "as_of"),
        Index("ix_miner_rolling_score_skill", "skill_score"),
    )

