"""Ledger access policy: challenge-response auth with vpermit + stake gate.

Validators must prove hotkey ownership via signed challenge, and meet
minimum requirements (validator_permit=True, stake >= threshold) to
receive a bearer token for ledger access.
"""

from __future__ import annotations

import os
import secrets
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

import bittensor as bt


@dataclass
class EligibilityResult:
    """Result of an eligibility check."""

    eligible: bool
    reason: str = ""


@dataclass
class _PendingChallenge:
    """A nonce waiting for response."""

    nonce: str
    hotkey: str
    created_at: float
    ttl: float = 120.0  # 2 minutes to respond

    @property
    def expired(self) -> bool:
        return time.time() > self.created_at + self.ttl


@dataclass
class _TokenEntry:
    """An issued bearer token."""

    hotkey: str
    created_at: float
    ttl: float

    @property
    def expired(self) -> bool:
        return time.time() > self.created_at + self.ttl


class AccessPolicy:
    """Challenge-response auth with metagraph-based eligibility checks.

    Fail-closed: any verification failure = reject.
    """

    def __init__(
        self,
        metagraph: Any,
        min_stake_threshold: int = 100_000,
        token_ttl: int = 3600,
        rate_limit_per_hour: int = 60,
        max_tokens: int = 500,
        max_pending_challenges: int = 1000,
    ):
        self.metagraph = metagraph
        self.min_stake_threshold = min_stake_threshold
        self.token_ttl = token_ttl
        self.rate_limit_per_hour = rate_limit_per_hour
        self.max_tokens = max_tokens
        self.max_pending_challenges = max_pending_challenges
        self._request_log_cleanup_interval = 300.0  # seconds
        self._request_log_last_cleanup = 0.0

        # Pending challenges: nonce -> _PendingChallenge
        self._challenges: dict[str, _PendingChallenge] = {}
        # Issued tokens: token -> _TokenEntry (LRU via OrderedDict)
        self._tokens: OrderedDict[str, _TokenEntry] = OrderedDict()
        # Rate limiting: hotkey -> list of request timestamps
        self._request_log: dict[str, list[float]] = {}

    def update_metagraph(self, metagraph: Any) -> None:
        """Update the metagraph reference (called after resync)."""
        self.metagraph = metagraph

    # -- Eligibility --

    def check_eligibility(self, hotkey: str) -> EligibilityResult:
        """Check if a hotkey meets ledger access requirements.

        Requirements:
        1. Hotkey exists in metagraph
        2. validator_permit == True  (skipped in test mode)
        3. stake >= min_stake_threshold  (skipped in test mode)
        """
        def _reject(reason: str) -> EligibilityResult:
            bt.logging.warning({"ledger_auth": {"event": "eligibility_rejected", "hotkey": hotkey[:16] if hotkey else "none", "reason": reason}})
            return EligibilityResult(eligible=False, reason=reason)

        if not hotkey:
            return _reject("empty_hotkey")

        try:
            hotkeys = list(self.metagraph.hotkeys)
        except Exception:
            return _reject("metagraph_unavailable")

        if hotkey not in hotkeys:
            return _reject("hotkey_not_found")

        # In test mode, only require hotkey presence in metagraph
        if os.environ.get("SPARKET_TEST_MODE", "").lower() in ("true", "1"):
            return EligibilityResult(eligible=True)

        idx = hotkeys.index(hotkey)

        # Check validator_permit
        try:
            vpermit = bool(self.metagraph.validator_permit[idx])
        except (IndexError, AttributeError):
            return _reject("vpermit_check_failed")

        if not vpermit:
            return _reject("no_validator_permit")

        # Check stake
        try:
            stake = float(self.metagraph.S[idx])
        except (IndexError, AttributeError):
            try:
                stake = float(self.metagraph.stake[idx])
            except (IndexError, AttributeError):
                return _reject("stake_check_failed")

        if stake < self.min_stake_threshold:
            return _reject(f"stake_too_low:{stake:.0f}<{self.min_stake_threshold}")

        return EligibilityResult(eligible=True)

    # -- Challenge-response --

    def issue_challenge(self, hotkey: str) -> str:
        """Generate a random nonce for a hotkey to sign."""
        # Clean expired challenges
        self._challenges = {
            k: v for k, v in self._challenges.items() if not v.expired
        }

        # Keep pending challenge map bounded.
        while len(self._challenges) >= self.max_pending_challenges:
            oldest_nonce = next(iter(self._challenges))
            self._challenges.pop(oldest_nonce, None)

        nonce = secrets.token_hex(32)
        self._challenges[nonce] = _PendingChallenge(
            nonce=nonce,
            hotkey=hotkey,
            created_at=time.time(),
        )
        return nonce

    def verify_response(
        self, hotkey: str, nonce: str, signature: str,
    ) -> str | None:
        """Verify a signed challenge and issue a bearer token.

        Returns:
            Bearer token string on success, None on failure.
        """
        hk = hotkey[:16] if hotkey else "none"

        # Look up challenge
        challenge = self._challenges.pop(nonce, None)
        if challenge is None:
            bt.logging.warning({"ledger_auth": {"event": "verify_failed", "hotkey": hk, "reason": "unknown_nonce"}})
            return None
        if challenge.expired:
            bt.logging.warning({"ledger_auth": {"event": "verify_failed", "hotkey": hk, "reason": "expired_nonce"}})
            return None
        if challenge.hotkey != hotkey:
            bt.logging.warning({"ledger_auth": {"event": "verify_failed", "hotkey": hk, "reason": "hotkey_mismatch"}})
            return None

        # Verify signature
        try:
            sig_bytes = bytes.fromhex(signature)
            keypair = bt.Keypair(ss58_address=hotkey)
            if not keypair.verify(nonce.encode(), sig_bytes):
                bt.logging.warning({"ledger_auth": {"event": "verify_failed", "hotkey": hk, "reason": "bad_signature"}})
                return None
        except Exception:
            bt.logging.warning({"ledger_auth": {"event": "verify_failed", "hotkey": hk, "reason": "signature_exception"}})
            return None

        # Issue token
        token = secrets.token_hex(32)
        entry = _TokenEntry(hotkey=hotkey, created_at=time.time(), ttl=self.token_ttl)

        # LRU eviction
        while len(self._tokens) >= self.max_tokens:
            self._tokens.popitem(last=False)

        self._tokens[token] = entry
        bt.logging.info({"ledger_auth": {"event": "token_issued", "hotkey": hk}})
        return token

    def validate_token(self, token: str) -> str | None:
        """Validate a bearer token. Returns the hotkey or None."""
        entry = self._tokens.get(token)
        if entry is None:
            return None
        if entry.expired:
            self._tokens.pop(token, None)
            return None
        # Move to end (LRU touch)
        self._tokens.move_to_end(token)
        return entry.hotkey

    # -- Rate limiting --

    def check_rate_limit(self, hotkey: str) -> bool:
        """Check if a hotkey is within rate limits. Returns True if allowed."""
        now = time.time()
        window = 3600.0  # 1 hour
        self._cleanup_request_log(now=now, window=window)

        log = self._request_log.get(hotkey, [])
        # Prune old entries
        log = [t for t in log if now - t < window]
        log.append(now)
        self._request_log[hotkey] = log

        allowed = len(log) <= self.rate_limit_per_hour
        if not allowed:
            bt.logging.warning({"ledger_auth": {"event": "rate_limited", "hotkey": hotkey[:16] if hotkey else "none", "requests_in_window": len(log)}})
        return allowed

    def _cleanup_request_log(self, now: float, window: float) -> None:
        """Periodically remove hotkeys with no recent requests."""
        if (now - self._request_log_last_cleanup) < self._request_log_cleanup_interval:
            return
        self._request_log_last_cleanup = now
        self._request_log = {
            hk: timestamps
            for hk, timestamps in self._request_log.items()
            if any((now - ts) < window for ts in timestamps)
        }


__all__ = ["AccessPolicy", "EligibilityResult"]
