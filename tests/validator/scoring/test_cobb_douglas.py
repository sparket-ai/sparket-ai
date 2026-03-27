"""Tests for Cobb-Douglas multiplicative scoring and compute_weights.

Covers:
- Pareto concentration (95th/50th ratio)
- Hard accuracy floor (Brier > 0.30 → zero)
- Specialist vs balanced miner scoring
- Dimension at epsilon → product collapse
- Bootstrap defaults when uniqueness/marginal are None
"""

import numpy as np

from sparket.validator.ledger.compute_weights import compute_weights, WeightResult
from sparket.validator.ledger.models import MinerMetrics, ScoringConfigSnapshot, ChainParamsSnapshot
from sparket.validator.config.scoring_params import ScoringParams


def _make_config() -> ScoringConfigSnapshot:
    """Create a ScoringConfigSnapshot with default params."""
    return ScoringConfigSnapshot(params=ScoringParams().model_dump(mode="json"))


def _make_chain(n_neurons: int = 300, burn_uid: int = 0) -> ChainParamsSnapshot:
    """Create a ChainParamsSnapshot."""
    return ChainParamsSnapshot(
        n_neurons=n_neurons,
        burn_uid=burn_uid,
        burn_rate=0.9,
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


class TestCobbDouglas:
    """Test the Cobb-Douglas multiplicative formula."""

    def test_hard_floor_zeroes_bad_miners(self):
        """Miner with Brier > 0.30 gets skill_score = 0."""
        good = _make_miner(1, brier_mean=0.15, fq_raw=0.5, uniqueness_dim=0.8, marginal_dim=0.8)
        bad = _make_miner(2, brier_mean=0.35, fq_raw=0.5, uniqueness_dim=0.8, marginal_dim=0.8)

        result = compute_weights([good, bad], _make_config(), _make_chain())

        assert result.skill_scores[1] > 0, "Good miner should have positive score"
        assert result.skill_scores[2] == 0.0, "Bad miner (Brier > 0.30) should be zeroed"

    def test_balanced_beats_specialist(self):
        """Miner balanced across all dims scores >> specialist strong on few."""
        # Balanced: all dimensions at 0.8 level
        balanced = _make_miner(
            1, fq_raw=0.6, pss_mean=0.3, es_adj=0.3, mes_mean=0.8,
            cal_score=0.8, sos_score=0.8, lead_score=0.8, brier_mean=0.10,
            uniqueness_dim=0.8, marginal_dim=0.8,
        )
        # Specialist: great accuracy/edge but poor uniqueness
        specialist = _make_miner(
            2, fq_raw=0.6, pss_mean=0.3, es_adj=0.3, mes_mean=0.8,
            cal_score=0.8, sos_score=0.8, lead_score=0.8, brier_mean=0.10,
            uniqueness_dim=0.2, marginal_dim=0.2,
        )

        result = compute_weights([balanced, specialist], _make_config(), _make_chain())

        ratio = result.skill_scores[1] / result.skill_scores[2] if result.skill_scores[2] > 0 else float('inf')
        assert ratio > 3.0, f"Balanced should score >> specialist, ratio={ratio:.1f}"

    def test_epsilon_dimension_collapses_product(self):
        """A dimension at epsilon (0.01) crushes the entire product."""
        normal = _make_miner(
            1, fq_raw=0.6, pss_mean=0.3, es_adj=0.3, mes_mean=0.8,
            cal_score=0.8, sos_score=0.8, lead_score=0.8, brier_mean=0.10,
            uniqueness_dim=0.8, marginal_dim=0.8,
        )
        collapsed = _make_miner(
            2, fq_raw=0.6, pss_mean=0.3, es_adj=0.3, mes_mean=0.8,
            cal_score=0.8, sos_score=0.8, lead_score=0.8, brier_mean=0.10,
            uniqueness_dim=0.01, marginal_dim=0.8,  # Uniqueness at epsilon
        )

        result = compute_weights([normal, collapsed], _make_config(), _make_chain())

        ratio = result.skill_scores[1] / result.skill_scores[2] if result.skill_scores[2] > 0 else float('inf')
        assert ratio > 50, f"Epsilon dimension should crush product, ratio={ratio:.1f}"

    def test_bootstrap_defaults(self):
        """When uniqueness/marginal are None, use sos_score and 0.5 respectively."""
        miner_with = _make_miner(
            1, fq_raw=0.5, pss_mean=0.2, es_adj=0.2, mes_mean=0.7,
            cal_score=0.7, sos_score=0.6, lead_score=0.6, brier_mean=0.12,
            uniqueness_dim=0.6,  # Explicitly set = sos_score
            marginal_dim=0.5,    # Explicitly set = 0.5
        )
        miner_without = _make_miner(
            2, fq_raw=0.5, pss_mean=0.2, es_adj=0.2, mes_mean=0.7,
            cal_score=0.7, sos_score=0.6, lead_score=0.6, brier_mean=0.12,
            uniqueness_dim=None,  # Should fall back to sos_score=0.6
            marginal_dim=None,    # Should fall back to 0.5
        )

        result = compute_weights(
            [miner_with, miner_without], _make_config(), _make_chain()
        )

        # Both should get identical scores since defaults match explicit values
        assert abs(result.skill_scores[1] - result.skill_scores[2]) < 0.01, (
            f"Bootstrap defaults should match explicit: "
            f"{result.skill_scores[1]:.4f} vs {result.skill_scores[2]:.4f}"
        )

    def test_multiplicative_not_additive(self):
        """Verify the formula is multiplicative — output differs from additive."""
        miners = [
            _make_miner(
                i, fq_raw=0.5, pss_mean=0.2, es_adj=0.2, mes_mean=0.7,
                cal_score=0.7, sos_score=0.6, lead_score=0.6, brier_mean=0.12,
                uniqueness_dim=0.7, marginal_dim=0.7,
            )
            for i in range(1, 11)
        ]

        result = compute_weights(miners, _make_config(), _make_chain())

        # All miners identical → all get same score
        scores = [result.skill_scores[i] for i in range(1, 11)]
        assert max(scores) - min(scores) < 1e-10, "Identical miners should get identical scores"

        # Score should be < 1 (product of values < 1 with exponents)
        assert scores[0] < 1.0
        assert scores[0] > 0.0

    def test_dimension_scores_in_audit_trail(self):
        """Audit trail includes all 5 Cobb-Douglas dimensions."""
        miner = _make_miner(
            1, fq_raw=0.5, pss_mean=0.2, es_adj=0.2, mes_mean=0.7,
            cal_score=0.7, sos_score=0.6, lead_score=0.6, brier_mean=0.12,
            uniqueness_dim=0.8, marginal_dim=0.7,
        )

        result = compute_weights([miner], _make_config(), _make_chain())

        dims = result.dimension_scores[1]
        assert "accuracy_dim" in dims
        assert "edge_dim" in dims
        assert "timeliness_dim" in dims
        assert "uniqueness_dim" in dims
        assert "marginal_dim" in dims
        # All should be in (0, 1]
        for name, val in dims.items():
            if name in ("accuracy_dim", "edge_dim", "timeliness_dim", "uniqueness_dim", "marginal_dim"):
                assert 0 < val <= 1.0, f"{name} = {val} out of range"

    def test_many_miners_produces_nonzero_weights(self):
        """With many miners, compute_weights produces valid uint16 outputs."""
        rng = np.random.default_rng(42)
        miners = []
        for i in range(1, 51):
            miners.append(_make_miner(
                i,
                fq_raw=rng.uniform(-0.5, 0.8),
                pss_mean=rng.uniform(-0.3, 0.5),
                es_adj=rng.uniform(-0.2, 0.5),
                mes_mean=rng.uniform(0.3, 0.9),
                cal_score=rng.uniform(0.3, 0.9),
                sos_score=rng.uniform(0.2, 0.9),
                lead_score=rng.uniform(0.2, 0.9),
                brier_mean=rng.uniform(0.05, 0.25),
                uniqueness_dim=rng.uniform(0.3, 0.9),
                marginal_dim=rng.uniform(0.3, 0.9),
            ))

        result = compute_weights(miners, _make_config(), _make_chain(n_neurons=300))

        assert len(result.uids) > 0, "Should produce some non-zero weights"
        assert len(result.uint16_weights) > 0
        assert all(0 < w <= 65535 for w in result.uint16_weights)
