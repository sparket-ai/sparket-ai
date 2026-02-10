"""Tests for validator/comms.py - Time-based token rotation and verification."""

from __future__ import annotations

import os
import time
from unittest.mock import MagicMock, patch

import pytest

from sparket.validator.comms import ValidatorComms


class TestValidatorComms:
    """Tests for ValidatorComms class."""

    def test_init_with_defaults(self):
        """Initializes with default values."""
        comms = ValidatorComms(proxy_url=None, require_token=True)
        assert comms.proxy_url is None
        assert comms.require_token is True
        assert comms.token_rotation_seconds == 3600
        assert comms._secret is not None
        assert len(comms._secret) == 32

    def test_init_with_proxy_url(self):
        """Initializes with custom proxy URL."""
        comms = ValidatorComms(proxy_url="https://proxy.example.com", require_token=False)
        assert comms.proxy_url == "https://proxy.example.com"
        assert comms.require_token is False

    def test_init_with_custom_rotation(self):
        """Initializes with custom rotation seconds."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=1800)
        assert comms.token_rotation_seconds == 1800

    def test_rotation_minimum_is_sixty(self):
        """Rotation seconds has minimum value of 60."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=10)
        assert comms.token_rotation_seconds == 60

        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=-5)
        assert comms.token_rotation_seconds == 60

    def test_init_with_env_secret(self):
        """Uses secret from environment variable if set."""
        with patch.dict(os.environ, {"SPARKET_VALIDATOR_PUSH_SECRET": "my_secret_key"}):
            comms = ValidatorComms(proxy_url=None, require_token=True)
            assert comms._secret == b"my_secret_key"

    def test_current_token_generates_token(self):
        """current_token generates a SHA256 hex digest."""
        comms = ValidatorComms(proxy_url=None, require_token=True)
        token = comms.current_token()
        assert isinstance(token, str)
        assert len(token) == 64  # SHA256 hex digest

    def test_current_token_same_within_epoch(self):
        """Same token within same time epoch."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        base = 1000000 * 3600  # start of an epoch boundary
        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = base + 100
            token_a = comms.current_token()
            mock_time.time.return_value = base + 200
            token_b = comms.current_token()

        assert token_a == token_b

    def test_current_token_different_across_epochs(self):
        """Different tokens across time epochs."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 0.0
            token_epoch0 = comms.current_token()
            comms._last_epoch = None  # clear cache
            mock_time.time.return_value = 3600.0
            token_epoch1 = comms.current_token()
            comms._last_epoch = None
            mock_time.time.return_value = 7200.0
            token_epoch2 = comms.current_token()

        assert token_epoch0 != token_epoch1 != token_epoch2

    def test_current_token_caches_result(self):
        """Token is cached per epoch."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 100.0
            token1 = comms.current_token()
            token2 = comms.current_token()

        assert token1 == token2
        assert comms._last_epoch == 0  # epoch for t=100 with rotation=3600
        assert comms._cached_token == token1


class TestVerifyToken:
    """Tests for token verification."""

    def test_verify_when_require_token_false(self):
        """Always returns True when require_token is False."""
        comms = ValidatorComms(proxy_url=None, require_token=False)
        assert comms.verify_token(token=None) is True
        assert comms.verify_token(token="garbage") is True
        assert comms.verify_token(token="") is True

    def test_verify_rejects_none_token(self):
        """Rejects None token when require_token is True."""
        comms = ValidatorComms(proxy_url=None, require_token=True)
        assert comms.verify_token(token=None) is False

    def test_verify_rejects_empty_token(self):
        """Rejects empty string token."""
        comms = ValidatorComms(proxy_url=None, require_token=True)
        assert comms.verify_token(token="") is False

    def test_verify_accepts_current_epoch_token(self):
        """Accepts token from current epoch."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 3700.0  # epoch 1
            token = comms.current_token()
            assert comms.verify_token(token=token) is True

    def test_verify_accepts_previous_epoch_token(self):
        """Accepts token from previous epoch (for clock drift)."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 100.0  # epoch 0
            token_epoch0 = comms.current_token()

            # Move to epoch 1 - should still accept epoch 0 token
            comms._last_epoch = None
            mock_time.time.return_value = 3700.0
            assert comms.verify_token(token=token_epoch0) is True

    def test_verify_rejects_old_epoch_token(self):
        """Rejects token from too old epoch."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 100.0  # epoch 0
            token_epoch0 = comms.current_token()

            # Move to epoch 2 - epoch 0 token is too old
            comms._last_epoch = None
            mock_time.time.return_value = 7300.0
            assert comms.verify_token(token=token_epoch0) is False

    def test_verify_rejects_garbage_token(self):
        """Rejects completely invalid token."""
        comms = ValidatorComms(proxy_url=None, require_token=True)
        assert comms.verify_token(token="not_a_valid_token") is False

    def test_verify_at_epoch_boundary(self):
        """Handles verification at epoch boundary."""
        comms = ValidatorComms(proxy_url=None, require_token=True, token_rotation_seconds=3600)

        with patch("sparket.validator.comms.time") as mock_time:
            mock_time.time.return_value = 3599.0  # end of epoch 0
            token_epoch0 = comms.current_token()

            comms._last_epoch = None
            mock_time.time.return_value = 3600.0  # start of epoch 1
            token_epoch1 = comms.current_token()

            # At epoch 1, both should be valid (current + previous)
            assert comms.verify_token(token=token_epoch0) is True
            assert comms.verify_token(token=token_epoch1) is True


class TestAdvertisedEndpoint:
    """Tests for advertised_endpoint method."""

    def test_returns_proxy_url_when_set(self):
        """Returns proxy URL when configured."""
        comms = ValidatorComms(proxy_url="https://proxy.example.com:8080", require_token=False)

        mock_axon = MagicMock()
        mock_axon.ip = "192.168.1.1"
        mock_axon.port = 9000

        result = comms.advertised_endpoint(axon=mock_axon)

        assert result == {"url": "https://proxy.example.com:8080"}

    def test_returns_axon_details_when_no_proxy(self):
        """Returns axon host/port when no proxy configured."""
        comms = ValidatorComms(proxy_url=None, require_token=False)

        mock_axon = MagicMock()
        mock_axon.external_ip = None
        mock_axon.external_port = None
        mock_axon.ip = "192.168.1.1"
        mock_axon.port = 9000

        result = comms.advertised_endpoint(axon=mock_axon)

        assert result == {"host": "192.168.1.1", "port": 9000}

    def test_defaults_when_axon_missing_attributes(self):
        """Uses defaults when axon missing ip/port."""
        comms = ValidatorComms(proxy_url=None, require_token=False)

        mock_axon = MagicMock(spec=[])

        result = comms.advertised_endpoint(axon=mock_axon)

        assert result == {"host": "127.0.0.1", "port": 0}

    def test_handles_none_ip(self):
        """Handles None ip gracefully."""
        comms = ValidatorComms(proxy_url=None, require_token=False)

        mock_axon = MagicMock()
        mock_axon.external_ip = None
        mock_axon.external_port = None
        mock_axon.ip = None
        mock_axon.port = 8080

        result = comms.advertised_endpoint(axon=mock_axon)

        assert result == {"host": "127.0.0.1", "port": 8080}

    def test_handles_none_port(self):
        """Handles None port gracefully."""
        comms = ValidatorComms(proxy_url=None, require_token=False)

        mock_axon = MagicMock()
        mock_axon.external_ip = None
        mock_axon.external_port = None
        mock_axon.ip = "10.0.0.1"
        mock_axon.port = None

        result = comms.advertised_endpoint(axon=mock_axon)

        assert result == {"host": "10.0.0.1", "port": 0}
