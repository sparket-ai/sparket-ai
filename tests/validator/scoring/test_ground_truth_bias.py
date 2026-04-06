"""Tests for sportsbook bias estimation."""

from decimal import Decimal

import pytest

from sparket.validator.scoring.ground_truth.bias import (
    BiasUpdateInput,
    BiasState,
    BiasKey,
    make_bias_key,
    get_initial_bias,
    BiasEstimator,
    compute_bias_input_hash,
)


class TestMakeBiasKey:
    """Tests for make_bias_key function."""

    def test_creates_tuple(self):
        """Should create a tuple key."""
        key = make_bias_key(1, 2, "moneyline")
        assert key == (1, 2, "moneyline")

    def test_key_type(self):
        """Should return BiasKey type (tuple)."""
        key = make_bias_key(1, 2, "spread")
        assert isinstance(key, tuple)
        assert len(key) == 3


class TestGetInitialBias:
    """Tests for get_initial_bias function."""

    def test_default_values(self):
        """Should return unbiased initial state."""
        state = get_initial_bias()
        assert state.bias_factor == Decimal("1.0")
        assert state.variance == Decimal("0.01")
        assert state.sample_count == 0
        assert state.version == 1

    def test_returns_bias_state(self):
        """Should return BiasState instance."""
        state = get_initial_bias()
        assert isinstance(state, BiasState)


class TestBiasEstimator:
    """Tests for BiasEstimator class."""

    @pytest.fixture
    def estimator(self):
        """Create estimator instance."""
        return BiasEstimator()

    def test_empty_observations(self, estimator):
        """Empty observations should return empty updates."""
        updates = estimator.compute_batch_updates([], {})
        assert updates == {}

    def test_new_key_initialized(self, estimator):
        """New key should get initial bias state."""
        obs = BiasUpdateInput(
            sportsbook_id=1,
            sport_id=1,
            market_kind="moneyline",
            book_prob=Decimal("0.6"),
            outcome_hit=1,
        )
        updates = estimator.compute_batch_updates([obs], {})

        key = make_bias_key(1, 1, "moneyline")
        assert key in updates
        assert updates[key].sportsbook_id == 1
        assert updates[key].sport_id == 1
        assert updates[key].market_kind == "moneyline"

    def test_sample_count_increments(self, estimator):
        """Sample count should increment with observations."""
        obs1 = BiasUpdateInput(1, 1, "ml", Decimal("0.5"), 1)
        obs2 = BiasUpdateInput(1, 1, "ml", Decimal("0.5"), 0)

        updates = estimator.compute_batch_updates([obs1, obs2], {})
        key = make_bias_key(1, 1, "ml")

        assert updates[key].sample_count == 2

    def test_version_increments(self, estimator):
        """Version should increment with each update."""
        key = make_bias_key(1, 1, "ml")
        initial = BiasState(1, 1, "ml", Decimal("1.0"), Decimal("0.01"), Decimal("0"), 10, 5)

        obs = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 1)
        updates = estimator.compute_batch_updates([obs], {key: initial})

        assert updates[key].version == 6  # 5 + 1

    def test_bias_adjustment_on_hit(self, estimator):
        """Bias should adjust when outcome hits."""
        # If book says 60% and outcome hits, bias should adjust
        obs = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 1)
        updates = estimator.compute_batch_updates([obs], {})

        key = make_bias_key(1, 1, "ml")
        # Book said 60%, outcome was 100%, so bias > 1 (underestimated)
        # But with EMA, movement is gradual
        assert updates[key].bias_factor != Decimal("1.0")

    def test_bias_adjustment_on_miss(self, estimator):
        """Bias should adjust when outcome misses."""
        # If book says 60% and outcome misses, bias adjusts differently
        obs = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 0)
        updates = estimator.compute_batch_updates([obs], {})

        key = make_bias_key(1, 1, "ml")
        # Book said 60%, outcome was 0%, so bias should decrease (overestimated)
        assert updates[key].bias_factor != Decimal("1.0")

    def test_bias_clamped_high(self, estimator):
        """Bias factor should be clamped to max."""
        # Create extreme adjustment scenario
        obs = BiasUpdateInput(1, 1, "ml", Decimal("0.1"), 1)  # Very low prob, hit
        current = BiasState(1, 1, "ml", Decimal("1.9"), Decimal("0.01"), Decimal("0"), 1000, 1)

        updates = estimator.compute_batch_updates([obs], {make_bias_key(1, 1, "ml"): current})
        key = make_bias_key(1, 1, "ml")

        # Should be clamped to max (default 2.0)
        assert updates[key].bias_factor <= Decimal("2.0")

    def test_bias_clamped_low(self, estimator):
        """Bias factor should be clamped to min."""
        # Create extreme adjustment scenario
        obs = BiasUpdateInput(1, 1, "ml", Decimal("0.9"), 0)  # High prob, miss
        current = BiasState(1, 1, "ml", Decimal("0.6"), Decimal("0.01"), Decimal("0"), 1000, 1)

        updates = estimator.compute_batch_updates([obs], {make_bias_key(1, 1, "ml"): current})
        key = make_bias_key(1, 1, "ml")

        # Should be clamped to min (default 0.5)
        assert updates[key].bias_factor >= Decimal("0.5")

    def test_multiple_keys_updated(self, estimator):
        """Multiple keys should be updated independently."""
        obs1 = BiasUpdateInput(1, 1, "ml", Decimal("0.5"), 1)
        obs2 = BiasUpdateInput(2, 1, "ml", Decimal("0.5"), 0)

        updates = estimator.compute_batch_updates([obs1, obs2], {})

        key1 = make_bias_key(1, 1, "ml")
        key2 = make_bias_key(2, 1, "ml")

        assert key1 in updates
        assert key2 in updates
        assert updates[key1].sportsbook_id == 1
        assert updates[key2].sportsbook_id == 2

    def test_deterministic_ordering(self, estimator):
        """Results should be same regardless of input order."""
        obs1 = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 1)
        obs2 = BiasUpdateInput(2, 1, "ml", Decimal("0.4"), 0)

        result1 = estimator.compute_batch_updates([obs1, obs2], {})
        result2 = estimator.compute_batch_updates([obs2, obs1], {})

        key1 = make_bias_key(1, 1, "ml")
        key2 = make_bias_key(2, 1, "ml")

        assert result1[key1].bias_factor == result2[key1].bias_factor
        assert result1[key2].bias_factor == result2[key2].bias_factor


class TestBiasUnbiasedness:
    """Tests that the bias estimator converges to 1.0 for calibrated books."""

    @pytest.fixture
    def estimator(self):
        return BiasEstimator()

    def test_converges_for_fair_coin(self, estimator):
        """Bias should converge to ~1.0 for a perfectly calibrated 50% book."""
        import random
        random.seed(42)

        key = make_bias_key(1, 1, "total")
        state = BiasState(1, 1, "total", Decimal("1.0"), Decimal("0.01"), Decimal("0"), 0, 1)

        for _ in range(2000):
            hit = 1 if random.random() < 0.5 else 0
            obs = BiasUpdateInput(1, 1, "total", Decimal("0.5"), hit)
            updates = estimator.compute_batch_updates([obs], {key: state})
            state = updates[key]

        # Should converge to 1.0 ± 0.1 for an unbiased book
        assert abs(float(state.bias_factor) - 1.0) < 0.1, (
            f"Bias factor drifted to {state.bias_factor} for a fair 50% book "
            f"(expected ~1.0). This indicates the EMA update is biased."
        )

    def test_converges_for_60pct_book(self, estimator):
        """Bias should converge to ~1.0 for a calibrated 60% book."""
        import random
        random.seed(123)

        key = make_bias_key(1, 1, "ml")
        state = BiasState(1, 1, "ml", Decimal("1.0"), Decimal("0.01"), Decimal("0"), 0, 1)

        for _ in range(2000):
            hit = 1 if random.random() < 0.6 else 0
            obs = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), hit)
            updates = estimator.compute_batch_updates([obs], {key: state})
            state = updates[key]

        assert abs(float(state.bias_factor) - 1.0) < 0.1, (
            f"Bias factor drifted to {state.bias_factor} for a calibrated 60% book"
        )


class TestIsBiasTrusted:
    """Tests for is_bias_trusted method."""

    @pytest.fixture
    def estimator(self):
        return BiasEstimator()

    def test_low_samples_untrusted(self, estimator):
        """Low sample count should be untrusted."""
        state = BiasState(1, 1, "ml", Decimal("1.0"), Decimal("0.01"), Decimal("0"), 5, 1)
        assert not estimator.is_bias_trusted(state)

    def test_high_samples_trusted(self, estimator):
        """High sample count should be trusted."""
        state = BiasState(1, 1, "ml", Decimal("1.0"), Decimal("0.01"), Decimal("0"), 1000, 1)
        assert estimator.is_bias_trusted(state)


class TestComputeBiasInputHash:
    """Tests for compute_bias_input_hash function."""

    def test_deterministic(self):
        """Same inputs should produce same hash."""
        obs = [
            BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 1),
            BiasUpdateInput(2, 1, "ml", Decimal("0.4"), 0),
        ]
        hash1 = compute_bias_input_hash(obs)
        hash2 = compute_bias_input_hash(obs)
        assert hash1 == hash2

    def test_order_independent(self):
        """Order should not affect hash."""
        obs1 = BiasUpdateInput(1, 1, "ml", Decimal("0.6"), 1)
        obs2 = BiasUpdateInput(2, 1, "ml", Decimal("0.4"), 0)

        hash_a = compute_bias_input_hash([obs1, obs2])
        hash_b = compute_bias_input_hash([obs2, obs1])
        assert hash_a == hash_b

    def test_returns_hex_string(self):
        """Should return 64-character hex string."""
        obs = [BiasUpdateInput(1, 1, "ml", Decimal("0.5"), 1)]
        result = compute_bias_input_hash(obs)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_empty_observations(self):
        """Empty observations should produce valid hash."""
        result = compute_bias_input_hash([])
        assert len(result) == 64

