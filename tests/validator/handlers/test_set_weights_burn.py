"""Tests for burn rate functionality in SetWeightsHandler."""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from sparket.validator.handlers.core.weights.set_weights import SetWeightsHandler


class TestGetBurnUid:
    """Tests for _get_burn_uid method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MagicMock()
        self.handler = SetWeightsHandler(database=self.db)
        self.validator = MagicMock()
        self.validator.config.netuid = 42

    def test_returns_uid_when_burn_hotkey_registered(self):
        """Should return UID when burn hotkey is in metagraph."""
        burn_hotkey = "5BurnHotkey123"
        self.validator.subtensor.get_subnet_owner_hotkey.return_value = burn_hotkey
        self.validator.metagraph.hotkeys = ["hk0", "hk1", burn_hotkey, "hk3"]

        result = self.handler._get_burn_uid(self.validator)

        assert result == 2
        self.validator.subtensor.get_subnet_owner_hotkey.assert_called_once_with(netuid=42)

    def test_returns_none_when_subtensor_returns_none(self):
        """Should return None when subtensor can't find owner hotkey."""
        self.validator.subtensor.get_subnet_owner_hotkey.return_value = None

        result = self.handler._get_burn_uid(self.validator)

        assert result is None

    def test_returns_none_when_burn_hotkey_not_registered(self):
        """Should return None when burn hotkey not in metagraph."""
        burn_hotkey = "5BurnHotkey123"
        self.validator.subtensor.get_subnet_owner_hotkey.return_value = burn_hotkey
        self.validator.metagraph.hotkeys = ["hk0", "hk1", "hk2"]  # No burn_hotkey

        result = self.handler._get_burn_uid(self.validator)

        assert result is None

    def test_returns_none_on_exception(self):
        """Should return None and not raise on exception."""
        self.validator.subtensor.get_subnet_owner_hotkey.side_effect = Exception("Network error")

        result = self.handler._get_burn_uid(self.validator)

        assert result is None


class TestApplyBurnRate:
    """Tests for _apply_burn_rate method."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MagicMock()
        self.handler = SetWeightsHandler(database=self.db)

    def test_90_percent_burn_rate(self):
        """90% burn rate should give 90% to burn_uid, 10% to miners proportionally.
        
        Note: burn_uid (subnet owner) has 0 weight initially since it's not mining.
        """
        # burn_uid=0 is the subnet owner (not mining, weight=0)
        # miners at uid 1-3 have scores
        raw_weights = np.array([0.0, 0.5, 0.3, 0.2], dtype=np.float32)
        burn_uid = 0
        burn_rate = 0.9

        result = self.handler._apply_burn_rate(raw_weights, burn_uid, burn_rate)

        # Burn UID should get 90%
        assert np.isclose(result[burn_uid], 0.9)
        # Other miners scaled down by (1 - 0.9) = 0.1
        assert np.isclose(result[1], 0.05)  # 0.5 * 0.1
        assert np.isclose(result[2], 0.03)  # 0.3 * 0.1
        assert np.isclose(result[3], 0.02)  # 0.2 * 0.1
        # Total should still sum to 1.0
        assert np.isclose(np.sum(result), 1.0)

    def test_50_percent_burn_rate(self):
        """50% burn rate should split evenly."""
        raw_weights = np.array([0.5, 0.5, 0.0, 0.0], dtype=np.float32)
        burn_uid = 2
        burn_rate = 0.5

        result = self.handler._apply_burn_rate(raw_weights, burn_uid, burn_rate)

        assert np.isclose(result[burn_uid], 0.5)
        assert np.isclose(result[0], 0.25)  # 0.5 * 0.5
        assert np.isclose(result[1], 0.25)  # 0.5 * 0.5
        assert np.isclose(np.sum(result), 1.0)

    def test_zero_burn_rate_returns_unchanged(self):
        """Zero burn rate should return original weights."""
        raw_weights = np.array([0.4, 0.3, 0.2, 0.1], dtype=np.float32)
        burn_uid = 0
        burn_rate = 0.0

        result = self.handler._apply_burn_rate(raw_weights, burn_uid, burn_rate)

        np.testing.assert_array_equal(result, raw_weights)

    def test_100_percent_burn_rate(self):
        """100% burn rate should give everything to burn_uid."""
        raw_weights = np.array([0.4, 0.3, 0.2, 0.1], dtype=np.float32)
        burn_uid = 1
        burn_rate = 1.0

        result = self.handler._apply_burn_rate(raw_weights, burn_uid, burn_rate)

        assert np.isclose(result[burn_uid], 1.0)
        assert np.isclose(result[0], 0.0)
        assert np.isclose(result[2], 0.0)
        assert np.isclose(result[3], 0.0)
        assert np.isclose(np.sum(result), 1.0)

    def test_preserves_relative_miner_weights(self):
        """Burn rate should preserve relative weights between miners."""
        raw_weights = np.array([0.0, 0.6, 0.3, 0.1], dtype=np.float32)
        burn_uid = 0
        burn_rate = 0.8

        result = self.handler._apply_burn_rate(raw_weights, burn_uid, burn_rate)

        # Relative ratios should be preserved: 6:3:1
        miner_weights = result[1:4]
        assert np.isclose(miner_weights[0] / miner_weights[2], 6.0)  # 0.6/0.1
        assert np.isclose(miner_weights[1] / miner_weights[2], 3.0)  # 0.3/0.1


class TestEmitWeightsWithBurn:
    """Tests for _emit_weights with burn rate integration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MagicMock()
        self.handler = SetWeightsHandler(database=self.db)
        self.validator = MagicMock()
        self.validator.config.netuid = 42
        self.validator.metagraph.n = 4
        self.validator.metagraph.uids = np.array([0, 1, 2, 3])

    @patch.object(SetWeightsHandler, '_get_burn_uid')
    def test_all_scores_zero_burns_100_percent(self, mock_get_burn_uid):
        """When all scores are zero, 100% should go to burn hotkey."""
        mock_get_burn_uid.return_value = 2
        scores = np.zeros(4, dtype=np.float32)

        # Capture what gets passed to process_weights_for_netuid
        with patch(
            'sparket.validator.handlers.core.weights.set_weights.process_weights_for_netuid'
        ) as mock_process:
            mock_process.return_value = (np.array([2]), np.array([1.0]))
            with patch(
                'sparket.validator.handlers.core.weights.set_weights.convert_weights_and_uids_for_emit'
            ) as mock_convert:
                mock_convert.return_value = (np.array([2]), np.array([65535]))
                self.validator.subtensor.set_weights.return_value = (True, "ok")

                self.handler._emit_weights(self.validator, scores)

                # Check that raw_weights passed has 100% at burn_uid
                call_args = mock_process.call_args
                raw_weights = call_args.kwargs['weights']
                assert np.isclose(raw_weights[2], 1.0)
                assert np.isclose(np.sum(raw_weights), 1.0)

    @patch.object(SetWeightsHandler, '_get_burn_uid')
    def test_all_scores_zero_no_burn_uid_skips_emission(self, mock_get_burn_uid):
        """When all scores zero and no burn UID, should skip emission."""
        mock_get_burn_uid.return_value = None
        scores = np.zeros(4, dtype=np.float32)

        self.handler._emit_weights(self.validator, scores)

        # set_weights should NOT be called
        self.validator.subtensor.set_weights.assert_not_called()

    @patch.object(SetWeightsHandler, '_get_burn_uid')
    @patch.object(SetWeightsHandler, '_apply_burn_rate')
    def test_with_scores_skips_burn_when_rate_zero(
        self, mock_apply_burn, mock_get_burn_uid
    ):
        """With burn_rate=0.0 (v0.1.2 default), should NOT apply burn rate."""
        mock_get_burn_uid.return_value = 0

        scores = np.array([0.4, 0.3, 0.2, 0.1], dtype=np.float32)

        with patch(
            'sparket.validator.handlers.core.weights.set_weights.process_weights_for_netuid'
        ) as mock_process:
            mock_process.return_value = (self.validator.metagraph.uids, np.array([0.4, 0.3, 0.2, 0.1]))
            with patch(
                'sparket.validator.handlers.core.weights.set_weights.convert_weights_and_uids_for_emit'
            ) as mock_convert:
                mock_convert.return_value = (np.array([0, 1, 2, 3]), np.array([26213, 19660, 13106, 6553]))
                self.validator.subtensor.set_weights.return_value = (True, "ok")

                self.handler._emit_weights(self.validator, scores)

                # burn_rate=0.0 → _apply_burn_rate should NOT be called
                mock_apply_burn.assert_not_called()


class TestEmitWeightsIntegration:
    """Integration tests for full emit_weights flow."""

    def setup_method(self):
        """Set up test fixtures."""
        self.db = MagicMock()
        self.handler = SetWeightsHandler(database=self.db)
        self.validator = MagicMock()
        self.validator.config.netuid = 42
        self.validator.metagraph.n = 256
        self.validator.metagraph.uids = np.arange(256)
        self.validator.metagraph.hotkeys = [f"hk{i}" for i in range(256)]

    def test_full_flow_no_burn(self):
        """Full integration test: scores -> normalized -> no burn (burn_rate=0.0)."""
        # Set up burn hotkey at UID 0
        burn_hotkey = "burn_hk"
        self.validator.metagraph.hotkeys[0] = burn_hotkey
        self.validator.subtensor.get_subnet_owner_hotkey.return_value = burn_hotkey

        # Create scores where only miners 1-5 have non-zero scores
        scores = np.zeros(256, dtype=np.float32)
        scores[1] = 0.5
        scores[2] = 0.3
        scores[3] = 0.1
        scores[4] = 0.07
        scores[5] = 0.03

        captured_weights = {}

        def capture_process(uids, weights, **kwargs):
            captured_weights['raw'] = weights.copy()
            return uids, weights

        with patch(
            'sparket.validator.handlers.core.weights.set_weights.process_weights_for_netuid',
            side_effect=capture_process
        ):
            with patch(
                'sparket.validator.handlers.core.weights.set_weights.convert_weights_and_uids_for_emit'
            ) as mock_convert:
                mock_convert.return_value = (np.arange(256), np.zeros(256, dtype=np.uint16))
                self.validator.subtensor.set_weights.return_value = (True, "ok")

                self.handler._emit_weights(self.validator, scores)

        # burn_rate=0.0: burn UID should get 0%, miners keep full share
        assert np.isclose(captured_weights['raw'][0], 0.0)

        # Miners get their full normalized proportional share
        assert np.isclose(captured_weights['raw'][1], 0.5, atol=1e-6)
        assert np.isclose(captured_weights['raw'][2], 0.3, atol=1e-6)

        # Total should still be 1.0
        assert np.isclose(np.sum(captured_weights['raw']), 1.0)

    def test_full_flow_zero_scores_100_burn(self):
        """When all scores zero, burn hotkey gets 100%."""
        burn_hotkey = "burn_hk"
        self.validator.metagraph.hotkeys[5] = burn_hotkey
        self.validator.subtensor.get_subnet_owner_hotkey.return_value = burn_hotkey

        scores = np.zeros(256, dtype=np.float32)

        captured_weights = {}

        def capture_process(uids, weights, **kwargs):
            captured_weights['raw'] = weights.copy()
            return uids, weights

        with patch(
            'sparket.validator.handlers.core.weights.set_weights.process_weights_for_netuid',
            side_effect=capture_process
        ):
            with patch(
                'sparket.validator.handlers.core.weights.set_weights.convert_weights_and_uids_for_emit'
            ) as mock_convert:
                mock_convert.return_value = (np.arange(256), np.zeros(256, dtype=np.uint16))
                self.validator.subtensor.set_weights.return_value = (True, "ok")

                self.handler._emit_weights(self.validator, scores)

        # Burn UID 5 should have 100%
        assert np.isclose(captured_weights['raw'][5], 1.0)
        # All others should be 0
        assert np.isclose(np.sum(captured_weights['raw']) - captured_weights['raw'][5], 0.0)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
