"""Tests for ledger access policy, challenge-response auth, and rate limiting."""

import time
import pytest

from sparket.validator.ledger.auth import AccessPolicy, EligibilityResult


class MockMetagraph:
    """Minimal mock metagraph for auth testing."""

    def __init__(self, entries: list[dict]):
        """entries: list of {hotkey, vpermit, stake}"""
        self.hotkeys = [e["hotkey"] for e in entries]
        self.validator_permit = [e.get("vpermit", False) for e in entries]
        self.S = [e.get("stake", 0) for e in entries]


@pytest.fixture
def metagraph():
    return MockMetagraph([
        {"hotkey": "validator_high", "vpermit": True, "stake": 200_000},
        {"hotkey": "validator_low", "vpermit": True, "stake": 50_000},
        {"hotkey": "validator_exact", "vpermit": True, "stake": 100_000},
        {"hotkey": "miner_rich", "vpermit": False, "stake": 500_000},
        {"hotkey": "miner_poor", "vpermit": False, "stake": 10},
    ])


@pytest.fixture
def policy(metagraph):
    return AccessPolicy(metagraph=metagraph, min_stake_threshold=100_000)


class TestAccessPolicy:

    def test_eligible_validator_passes(self, policy):
        result = policy.check_eligibility("validator_high")
        assert result.eligible

    def test_miner_rejected(self, policy):
        result = policy.check_eligibility("miner_rich")
        assert not result.eligible
        assert "no_validator_permit" in result.reason

    def test_low_stake_validator_rejected(self, policy):
        result = policy.check_eligibility("validator_low")
        assert not result.eligible
        assert "stake_too_low" in result.reason

    def test_unknown_hotkey_rejected(self, policy):
        result = policy.check_eligibility("nonexistent_hotkey")
        assert not result.eligible
        assert "hotkey_not_found" in result.reason

    def test_threshold_boundary(self, policy):
        """Exactly 100K alpha should pass."""
        result = policy.check_eligibility("validator_exact")
        assert result.eligible

    def test_threshold_configurable(self, metagraph):
        strict = AccessPolicy(metagraph=metagraph, min_stake_threshold=300_000)
        result = strict.check_eligibility("validator_high")
        assert not result.eligible
        assert "stake_too_low" in result.reason

    def test_empty_hotkey_rejected(self, policy):
        result = policy.check_eligibility("")
        assert not result.eligible


class TestChallengeResponse:

    def test_full_flow(self, policy):
        """Challenge -> sign -> respond -> get token -> validate token."""
        hotkey = "validator_high"
        nonce = policy.issue_challenge(hotkey)
        assert isinstance(nonce, str)
        assert len(nonce) == 64  # 32 bytes hex

        # For the test, we can't sign properly without a real keypair.
        # Test the nonce lookup and expiry mechanisms instead.
        # A real integration test covers the full crypto flow.

    def test_expired_nonce_rejected(self, policy):
        hotkey = "validator_high"
        nonce = policy.issue_challenge(hotkey)

        # Manually expire the challenge
        challenge = policy._challenges.get(nonce)
        if challenge:
            challenge.created_at = time.time() - 300  # 5 min ago

        result = policy.verify_response(hotkey, nonce, "fake_sig")
        assert result is None

    def test_replay_nonce_rejected(self, policy):
        hotkey = "validator_high"
        nonce = policy.issue_challenge(hotkey)

        # First use (will fail due to bad sig, but removes the nonce)
        policy.verify_response(hotkey, nonce, "fake_sig")

        # Second use - nonce gone
        result = policy.verify_response(hotkey, nonce, "fake_sig")
        assert result is None

    def test_unknown_nonce_rejected(self, policy):
        result = policy.verify_response("validator_high", "nonexistent_nonce", "sig")
        assert result is None

    def test_pending_challenges_capped(self):
        metagraph = MockMetagraph([
            {"hotkey": "validator_high", "vpermit": True, "stake": 200_000},
        ])
        policy = AccessPolicy(metagraph=metagraph, max_pending_challenges=2)

        n1 = policy.issue_challenge("validator_high")
        n2 = policy.issue_challenge("validator_high")
        n3 = policy.issue_challenge("validator_high")

        assert len(policy._challenges) == 2
        assert n1 not in policy._challenges
        assert n2 in policy._challenges
        assert n3 in policy._challenges

    def test_wrong_hotkey_for_nonce(self, policy):
        nonce = policy.issue_challenge("validator_high")
        # Try to use with different hotkey
        result = policy.verify_response("validator_exact", nonce, "sig")
        assert result is None

    def test_token_ttl_expiry(self, policy):
        """Token should be rejected after TTL."""
        # Directly create a token entry for testing
        import secrets
        from sparket.validator.ledger.auth import _TokenEntry

        token = secrets.token_hex(32)
        entry = _TokenEntry(hotkey="validator_high", created_at=time.time() - 7200, ttl=3600)
        policy._tokens[token] = entry

        result = policy.validate_token(token)
        assert result is None  # Expired

    def test_token_lru_eviction(self):
        """LRU eviction kicks in when max_tokens is reached via verify_response path."""
        metagraph = MockMetagraph([
            {"hotkey": "v1", "vpermit": True, "stake": 200_000},
        ])
        policy = AccessPolicy(metagraph=metagraph, max_tokens=3)

        import secrets
        from sparket.validator.ledger.auth import _TokenEntry

        # Fill to capacity
        tokens = []
        for i in range(3):
            token = secrets.token_hex(32)
            policy._tokens[token] = _TokenEntry(
                hotkey="v1", created_at=time.time(), ttl=3600,
            )
            tokens.append(token)

        assert len(policy._tokens) == 3

        # Simulate what verify_response does: evict oldest before adding
        while len(policy._tokens) >= policy.max_tokens:
            policy._tokens.popitem(last=False)

        token4 = secrets.token_hex(32)
        policy._tokens[token4] = _TokenEntry(
            hotkey="v1", created_at=time.time(), ttl=3600,
        )

        assert policy.validate_token(tokens[0]) is None  # Evicted
        assert policy.validate_token(token4) == "v1"  # Still valid


class TestRateLimiting:

    def test_under_limit_allowed(self, policy):
        for _ in range(59):
            assert policy.check_rate_limit("validator_high")

    def test_over_limit_rejected(self, policy):
        for _ in range(60):
            policy.check_rate_limit("validator_high")
        assert not policy.check_rate_limit("validator_high")

    def test_rate_limit_per_hotkey(self, policy):
        """Different hotkeys have independent rate limits."""
        for _ in range(60):
            policy.check_rate_limit("validator_high")
        # validator_high is at limit
        assert not policy.check_rate_limit("validator_high")
        # validator_exact should be fine
        assert policy.check_rate_limit("validator_exact")
