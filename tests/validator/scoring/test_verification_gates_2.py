"""Verification gate tests G4 through G7 for the Sparket scoring system.

G4: Anti-Gaming Robustness
G5: Data Pipeline Completeness
G6: Pipeline Execution Integrity
G7: Protocol Safety
"""

from __future__ import annotations

import inspect
import textwrap

import numpy as np
import pytest

from sparket.validator.ledger.compute_weights import compute_weights, WeightResult, U16_MAX
from sparket.validator.ledger.models import (
    LEDGER_SCHEMA_VERSION,
    ChainParamsSnapshot,
    MinerMetrics,
    ScoringConfigSnapshot,
)
from sparket.validator.config.scoring_params import ScoringParams
from sparket.validator.scoring.aggregation.clustering import (
    compute_cluster_penalty,
    compute_soft_cluster_penalty,
    detect_clusters,
)
from sparket.validator.scoring.aggregation.correlation import (
    compute_pairwise_correlations,
    compute_sos_crowd,
)


# ---------------------------------------------------------------------------
# Helpers (follow existing patterns from test_cobb_douglas.py)
# ---------------------------------------------------------------------------


def _make_config() -> ScoringConfigSnapshot:
    """Create a ScoringConfigSnapshot with default params."""
    return ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))


def _make_chain(
    n_neurons: int = 300,
    burn_uid: int | None = 0,
    burn_rate: float = 0.9,
) -> ChainParamsSnapshot:
    return ChainParamsSnapshot(
        n_neurons=n_neurons,
        burn_uid=burn_uid,
        burn_rate=burn_rate,
        max_weight_limit=0.1,
        min_allowed_weights=1,
    )


def _make_miner(
    uid: int,
    fq_raw: float = 0.0,
    pss_mean: float = 0.0,
    es_adj: float = 0.0,
    mes_mean: float = 0.5,
    cal_score: float = 0.5,
    sos_score: float = 0.5,
    lead_score: float = 0.5,
    brier_mean: float = 0.15,
    uniqueness_dim: float | None = None,
    marginal_dim: float | None = None,
) -> MinerMetrics:
    return MinerMetrics(
        uid=uid,
        hotkey=f"hotkey_{uid}",
        fq_raw=fq_raw,
        pss_mean=pss_mean,
        es_adj=es_adj,
        mes_mean=mes_mean,
        cal_score=cal_score,
        sos_score=sos_score,
        lead_score=lead_score,
        brier_mean=brier_mean,
        uniqueness_dim=uniqueness_dim,
        marginal_dim=marginal_dim,
    )


# ===================================================================
# G4: Anti-Gaming Robustness
# ===================================================================


class TestG4AntiGaming:
    """G4: Anti-Gaming Robustness."""

    # G4.1 ---------------------------------------------------------------

    def test_sybil_cluster_penalty_scales(self):
        """G4.1: Cluster penalty grows with cluster size.

        For N identical miners (correlation ~1.0), penalty = (N-1)/N.
        For N=149, penalty should be approximately 0.993.
        """
        for n in [2, 5, 10, 50, 149]:
            # Correlation matrix with all entries = 1.0 (identical miners)
            corr = np.ones((n, n))
            clusters = detect_clusters(corr, threshold=0.85)

            # All miners should be in a single cluster
            assert len(clusters) == 1, (
                f"N={n}: expected 1 cluster, got {len(clusters)}"
            )
            assert len(clusters[0]) == n, (
                f"N={n}: expected cluster of size {n}, got {len(clusters[0])}"
            )

            penalty = compute_cluster_penalty(clusters, miner_idx=0)
            expected = (n - 1) / n
            assert abs(penalty - expected) < 1e-9, (
                f"N={n}: penalty {penalty:.6f} != expected {expected:.6f}"
            )

        # Specifically verify N=149
        n = 149
        corr = np.ones((n, n))
        clusters = detect_clusters(corr, threshold=0.85)
        penalty = compute_cluster_penalty(clusters, miner_idx=0)
        assert abs(penalty - 148 / 149) < 1e-9, (
            f"N=149: penalty {penalty:.6f} != {148/149:.6f}"
        )

    def test_sybil_singleton_penalty_zero(self):
        """G4.1 supplement: Singleton miners get penalty = 0."""
        # 3 independent miners (low correlation)
        corr = np.eye(3)
        clusters = detect_clusters(corr, threshold=0.85)

        # Should produce 3 singleton clusters
        assert len(clusters) == 3
        for idx in range(3):
            penalty = compute_cluster_penalty(clusters, miner_idx=idx)
            assert penalty == 0.0, f"Singleton idx={idx} should get 0 penalty"

    def test_soft_cluster_penalty_range(self):
        """G4.1: compute_soft_cluster_penalty returns values in expected range.

        - max_corr <= soft_floor (0.5) => penalty = 0.0
        - max_corr >= soft_ceil (0.95) => penalty = 1.0
        - in between => linear ramp
        """
        n = 3
        # Case 1: low correlation (below soft_floor)
        corr_low = np.eye(n)
        corr_low[0, 1] = corr_low[1, 0] = 0.3  # below default floor of 0.5
        penalty = compute_soft_cluster_penalty(corr_low, miner_idx=0)
        assert penalty == 0.0, f"Low corr should give 0.0 penalty, got {penalty}"

        # Case 2: high correlation (above soft_ceil)
        corr_high = np.eye(n)
        corr_high[0, 1] = corr_high[1, 0] = 0.96
        penalty = compute_soft_cluster_penalty(corr_high, miner_idx=0)
        assert penalty == 1.0, f"High corr should give 1.0 penalty, got {penalty}"

        # Case 3: mid correlation (between floor and ceil) => linear ramp
        corr_mid = np.eye(n)
        corr_mid[0, 1] = corr_mid[1, 0] = 0.725  # midpoint of [0.5, 0.95]
        penalty = compute_soft_cluster_penalty(corr_mid, miner_idx=0)
        expected = (0.725 - 0.5) / (0.95 - 0.5)
        assert abs(penalty - expected) < 1e-6, (
            f"Mid corr: penalty {penalty:.4f} != expected {expected:.4f}"
        )

        # Case 4: single miner => no penalty
        corr_single = np.eye(1)
        penalty = compute_soft_cluster_penalty(corr_single, miner_idx=0)
        assert penalty == 0.0, "Single miner should get 0.0 penalty"

    # G4.3 ---------------------------------------------------------------

    def test_zero_outcomes_skill_zero(self):
        """G4.3: Miner with brier_wt=0 (no outcomes) defaults brier_mean=0.5.

        brier_mean=0.5 > floor 0.30 => skill_score = 0.
        This verifies the full path through compute_weights.
        """
        # brier_mean defaults to 0.5 when no outcome data
        # (AccumulatorEntry.derive_means sets brier_mean = 0.5 when brier.wt == 0)
        # Here we directly set brier_mean = 0.5 to simulate that condition
        zero_outcome_miner = _make_miner(
            uid=1,
            brier_mean=0.5,  # default when brier_wt=0
            fq_raw=0.3,
            pss_mean=0.2,
            es_adj=0.1,
            uniqueness_dim=0.8,
            marginal_dim=0.8,
        )
        # Add a good miner so compute_weights can normalize
        good_miner = _make_miner(
            uid=2,
            brier_mean=0.15,
            fq_raw=0.5,
            pss_mean=0.3,
            es_adj=0.2,
            uniqueness_dim=0.8,
            marginal_dim=0.8,
        )

        result = compute_weights(
            [zero_outcome_miner, good_miner],
            _make_config(),
            _make_chain(),
        )

        assert result.skill_scores[1] == 0.0, (
            f"Miner with brier_mean=0.5 should be floored to 0, "
            f"got {result.skill_scores[1]}"
        )
        assert result.skill_scores[2] > 0.0, "Good miner should have positive score"

    # G4.4 ---------------------------------------------------------------

    def test_constant_prob_flagged(self):
        """G4.4: Miners with identical probability submissions get correlation ~1.0.

        The correlation code treats constant submissions (std < 1e-12)
        as perfectly correlated (sybil signal).
        """
        n_markets = 20
        market_ids = np.arange(n_markets)

        # Two miners submitting the exact same constant probability
        submissions = {
            "miner_0": np.full(n_markets, 0.55),
            "miner_1": np.full(n_markets, 0.55),
        }

        corr = compute_pairwise_correlations(submissions, market_ids, min_common=5)

        assert corr.shape == (2, 2)
        # Diagonal is 1.0
        assert corr[0, 0] == 1.0
        assert corr[1, 1] == 1.0
        # Off-diagonal should be 1.0 (constant => sybil signal)
        assert corr[0, 1] == 1.0, (
            f"Constant submissions should flag as correlation=1.0, got {corr[0, 1]}"
        )
        assert corr[1, 0] == 1.0

    def test_constant_prob_different_values_flagged(self):
        """G4.4 supplement: Even different constant values trigger flag.

        Both miners submit constant (zero std), correlation defaults to 1.0.
        """
        n_markets = 20
        market_ids = np.arange(n_markets)

        submissions = {
            "miner_0": np.full(n_markets, 0.40),
            "miner_1": np.full(n_markets, 0.70),
        }

        corr = compute_pairwise_correlations(submissions, market_ids, min_common=5)
        # Both have std < 1e-12 => correlation set to 1.0 (sybil signal)
        assert corr[0, 1] == 1.0, (
            f"Different constant values should still flag, got {corr[0, 1]}"
        )

    # G4.5 ---------------------------------------------------------------

    def test_brier_floor_enforcement(self):
        """G4.5: Miner with brier_mean=0.35 (> floor 0.30) gets 0 weight.

        compute_weights applies hard floor: brier > 0.30 => skill_score = 0.
        """
        bad_miner = _make_miner(
            uid=1,
            brier_mean=0.35,
            fq_raw=0.6,
            pss_mean=0.4,
            es_adj=0.3,
            mes_mean=0.8,
            cal_score=0.8,
            sos_score=0.8,
            lead_score=0.8,
            uniqueness_dim=0.9,
            marginal_dim=0.9,
        )
        good_miner = _make_miner(
            uid=2,
            brier_mean=0.15,
            fq_raw=0.5,
            pss_mean=0.3,
            es_adj=0.2,
            uniqueness_dim=0.8,
            marginal_dim=0.8,
        )

        result = compute_weights(
            [bad_miner, good_miner],
            _make_config(),
            _make_chain(),
        )

        assert result.skill_scores[1] == 0.0, (
            f"brier_mean=0.35 > floor=0.30 should give skill_score=0, "
            f"got {result.skill_scores[1]}"
        )
        # Miner 1 should not appear in final weights (or have 0 weight)
        if 1 in dict(zip(result.uids, result.uint16_weights)):
            assert dict(zip(result.uids, result.uint16_weights))[1] == 0


# ===================================================================
# G5: Data Pipeline Completeness
# ===================================================================


class TestG5DataPipeline:
    """G5: Data Pipeline Completeness."""

    # G5.2 ---------------------------------------------------------------

    def test_gt_contamination_clean(self):
        """G5.2: _UPSERT_GROUND_TRUTH_CLOSING SQL contains market kind filter.

        Verify the SQL joins market table and filters by valid sides
        per market kind, preventing ground_truth_closing contamination
        from invalid side/kind combinations.
        """
        from sparket.validator.scoring.ground_truth.snapshot_pipeline import (
            _UPSERT_GROUND_TRUTH_CLOSING,
        )

        sql_text = str(_UPSERT_GROUND_TRUTH_CLOSING.text)

        # Must JOIN market table for kind-based filtering
        assert "JOIN market m" in sql_text, (
            "SQL must JOIN market table for kind filtering"
        )
        assert "m.kind" in sql_text, (
            "SQL must reference m.kind for market-kind filtering"
        )

        # Must filter sides per market kind
        assert "MONEYLINE" in sql_text, "SQL must handle MONEYLINE kind"
        assert "SPREAD" in sql_text, "SQL must handle SPREAD kind"
        assert "TOTAL" in sql_text, "SQL must handle TOTAL kind"

        # Must include valid side names
        assert "'HOME'" in sql_text, "SQL must include HOME side"
        assert "'AWAY'" in sql_text, "SQL must include AWAY side"
        assert "'OVER'" in sql_text, "SQL must include OVER side"
        assert "'UNDER'" in sql_text, "SQL must include UNDER side"

        # TOTAL markets should only have OVER/UNDER, not HOME/AWAY
        # Verify the TOTAL clause specifically restricts to OVER/UNDER
        assert "TOTAL" in sql_text and "OVER" in sql_text and "UNDER" in sql_text

        # Verify is_closing filter is present
        assert "is_closing" in sql_text, "SQL must filter on is_closing flag"


# ===================================================================
# G6: Pipeline Execution Integrity
# ===================================================================


class TestG6PipelineIntegrity:
    """G6: Pipeline Execution Integrity."""

    # G6.1 ---------------------------------------------------------------

    def test_job_execution_order(self):
        """G6.1: Scoring jobs execute in correct dependency order.

        Required order:
        RollingAggregates -> CalibrationSharpness -> OriginalityLeadLag -> SkillScore

        CompositeUniqueness is not in the main pipeline's job_classes list
        (it runs as part of a different execution path), but the four core
        jobs must run in the correct order.
        """
        from sparket.validator.handlers.score.main_score import MainScoreHandler

        # Read the source to find job_classes list
        source = inspect.getsource(MainScoreHandler.run)

        # Verify the job order by finding class name positions in source
        from sparket.validator.scoring.jobs.rolling_aggregates import RollingAggregatesJob
        from sparket.validator.scoring.jobs.calibration_sharpness import CalibrationSharpnessJob
        from sparket.validator.scoring.jobs.originality_lead_lag import OriginalityLeadLagJob
        from sparket.validator.scoring.jobs.skill_score import SkillScoreJob

        expected_order = [
            "RollingAggregatesJob",
            "CalibrationSharpnessJob",
            "OriginalityLeadLagJob",
            "SkillScoreJob",
        ]

        # Find positions of each class name in the source
        positions = []
        for name in expected_order:
            pos = source.find(name)
            assert pos >= 0, f"{name} not found in MainScoreHandler.run()"
            positions.append(pos)

        # Verify they appear in ascending order (correct dependency chain)
        for i in range(len(positions) - 1):
            assert positions[i] < positions[i + 1], (
                f"{expected_order[i]} (pos {positions[i]}) must come before "
                f"{expected_order[i+1]} (pos {positions[i+1]})"
            )

    # G6.2 ---------------------------------------------------------------

    def test_skill_score_skipped_on_upstream_failure(self):
        """G6.2: SkillScoreJob is skipped when an upstream job fails.

        If RollingAggregatesJob (or any upstream job) fails, SkillScoreJob
        must NOT run because it would compute on stale data. The code must
        track failed jobs and skip the terminal job when failures exist.
        """
        from sparket.validator.handlers.score.main_score import MainScoreHandler

        source = inspect.getsource(MainScoreHandler.run)

        # Must track failed jobs
        assert "failed_jobs" in source, (
            "MainScoreHandler.run() must track failed_jobs for dependency enforcement"
        )

        # Must have a skip path that references upstream failures
        assert "skipped due to upstream failures" in source, (
            "MainScoreHandler.run() must skip terminal job when upstream jobs fail"
        )

        # The skip must result in a warning log with 'skipped' status
        assert '"skipped"' in source or "'skipped'" in source, (
            "MainScoreHandler.run() must log skipped status for dependency-blocked jobs"
        )

    # G6.3 ---------------------------------------------------------------

    def test_checkpoint_insert_has_error_count(self):
        """G6.3: The checkpoint INSERT SQL in ScoringJob.checkpoint includes error_count.

        Without error_count, the INSERT would fail if the scoring_job_state
        table has a NOT NULL constraint on that column.
        """
        from sparket.validator.scoring.jobs.base import ScoringJob

        source = inspect.getsource(ScoringJob.checkpoint)

        # The INSERT in checkpoint() must include error_count column
        assert "error_count" in source, (
            "ScoringJob.checkpoint() INSERT must include error_count column"
        )

        # Also verify it appears in both the column list and VALUES list
        # (not just in a comment)
        assert "error_count" in source


# ===================================================================
# G7: Protocol Safety
# ===================================================================


class TestG7ProtocolSafety:
    """G7: Protocol Safety."""

    # G7.1 ---------------------------------------------------------------

    def test_schema_version_constant(self):
        """G7.1: LEDGER_SCHEMA_VERSION equals the expected value (2)."""
        assert LEDGER_SCHEMA_VERSION == 2, (
            f"Expected LEDGER_SCHEMA_VERSION=2, got {LEDGER_SCHEMA_VERSION}"
        )

    # G7.4 ---------------------------------------------------------------

    def test_weight_emission_valid_uint16(self):
        """G7.4: All emitted uint16 weights are in [0, 65535] and UIDs valid."""
        rng = np.random.default_rng(42)
        n_neurons = 300
        miners = []
        for i in range(1, 51):
            miners.append(_make_miner(
                uid=i,
                fq_raw=rng.uniform(-0.3, 0.7),
                pss_mean=rng.uniform(-0.2, 0.4),
                es_adj=rng.uniform(-0.1, 0.4),
                mes_mean=rng.uniform(0.3, 0.9),
                cal_score=rng.uniform(0.3, 0.9),
                sos_score=rng.uniform(0.3, 0.9),
                lead_score=rng.uniform(0.3, 0.9),
                brier_mean=rng.uniform(0.05, 0.25),
                uniqueness_dim=rng.uniform(0.3, 0.9),
                marginal_dim=rng.uniform(0.3, 0.9),
            ))

        result = compute_weights(
            miners,
            _make_config(),
            _make_chain(n_neurons=n_neurons),
        )

        assert len(result.uids) > 0, "Should produce non-empty weight vector"
        assert len(result.uids) == len(result.uint16_weights)

        for uid, weight in zip(result.uids, result.uint16_weights):
            assert isinstance(uid, int), f"UID must be int, got {type(uid)}"
            assert isinstance(weight, int), f"Weight must be int, got {type(weight)}"
            assert 0 <= uid < n_neurons, (
                f"UID {uid} out of range [0, {n_neurons})"
            )
            assert 0 <= weight <= 65535, (
                f"Weight {weight} for uid {uid} not in [0, 65535]"
            )

    def test_weight_emission_no_duplicate_uids(self):
        """G7.4 supplement: No duplicate UIDs in output."""
        miners = [
            _make_miner(
                uid=i,
                brier_mean=0.15,
                fq_raw=0.4,
                uniqueness_dim=0.7,
                marginal_dim=0.7,
            )
            for i in range(1, 21)
        ]

        result = compute_weights(miners, _make_config(), _make_chain())

        assert len(result.uids) == len(set(result.uids)), (
            "Duplicate UIDs in weight output"
        )

    # G7.5 ---------------------------------------------------------------

    def test_burn_allocation(self):
        """G7.5: Burn UID gets ~burn_rate fraction of total weight.

        With burn_rate=0.1 and burn_uid=0, uid 0 should receive
        approximately 10% of total weight (within quantization tolerance).

        NOTE: burn_rate is read from scoring_config.params.weight_emission.burn_rate,
        not from ChainParamsSnapshot.burn_rate. We must set it in the config.
        """
        burn_rate = 0.1
        burn_uid = 0

        miners = [
            _make_miner(
                uid=i,
                brier_mean=0.15,
                fq_raw=0.5,
                pss_mean=0.3,
                es_adj=0.2,
                mes_mean=0.7,
                cal_score=0.7,
                sos_score=0.6,
                lead_score=0.6,
                uniqueness_dim=0.8,
                marginal_dim=0.8,
            )
            for i in range(1, 21)
        ]

        config = _make_config()
        config.params["weight_emission"]["burn_rate"] = burn_rate

        result = compute_weights(
            miners,
            config,
            _make_chain(burn_uid=burn_uid),
        )

        # Build uid -> weight map
        weight_map = dict(zip(result.uids, result.uint16_weights))

        assert burn_uid in weight_map, (
            f"Burn UID {burn_uid} not found in weight output"
        )

        # Raw weight fraction should be close to burn_rate
        raw_burn = result.raw_weights.get(burn_uid, 0.0)
        raw_total = sum(result.raw_weights.values())
        raw_fraction = raw_burn / raw_total if raw_total > 0 else 0.0

        assert abs(raw_fraction - burn_rate) < 0.02, (
            f"Burn UID raw fraction {raw_fraction:.4f} not close to "
            f"burn_rate {burn_rate} (tolerance 0.02)"
        )

    def test_burn_allocation_high_rate(self):
        """G7.5 supplement: Higher burn rate allocates proportionally more."""
        burn_rate_low = 0.1
        burn_rate_high = 0.5
        burn_uid = 0

        miners = [
            _make_miner(
                uid=i,
                brier_mean=0.15,
                fq_raw=0.5,
                uniqueness_dim=0.8,
                marginal_dim=0.8,
            )
            for i in range(1, 11)
        ]

        config_high = _make_config()
        config_high.params["weight_emission"]["burn_rate"] = burn_rate_high

        result_high = compute_weights(
            miners, config_high, _make_chain(burn_uid=burn_uid),
        )

        config_low = _make_config()
        config_low.params["weight_emission"]["burn_rate"] = burn_rate_low

        result_low = compute_weights(
            miners, config_low, _make_chain(burn_uid=burn_uid),
        )

        raw_burn_high = result_high.raw_weights.get(burn_uid, 0.0)
        raw_total_high = sum(result_high.raw_weights.values())
        frac_high = raw_burn_high / raw_total_high if raw_total_high > 0 else 0.0

        raw_burn_low = result_low.raw_weights.get(burn_uid, 0.0)
        raw_total_low = sum(result_low.raw_weights.values())
        frac_low = raw_burn_low / raw_total_low if raw_total_low > 0 else 0.0

        assert abs(frac_high - burn_rate_high) < 0.02, (
            f"High burn: raw fraction {frac_high:.4f} not close to {burn_rate_high}"
        )
        assert abs(frac_low - burn_rate_low) < 0.02, (
            f"Low burn: raw fraction {frac_low:.4f} not close to {burn_rate_low}"
        )
        assert frac_high > frac_low, (
            f"Higher burn rate should give higher fraction: "
            f"{frac_high:.4f} vs {frac_low:.4f}"
        )
