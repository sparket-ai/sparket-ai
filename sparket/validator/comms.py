from __future__ import annotations

import hmac
import os
import secrets
import time
from typing import Any, Dict, Optional

import bittensor as bt


class ValidatorComms:
    """
    Manages validator-side announcement details and a rotating token for miner pushes.
    - Builds the advertised endpoint (proxy_url if set, else host/port from axon)
    - Maintains an HMAC token rotated on a time basis (default: 1 hour)
    - Verifies presented tokens from miners
    """

    def __init__(
        self,
        *,
        proxy_url: Optional[str],
        require_token: bool,
        token_rotation_seconds: int = 3600,
    ) -> None:
        self.proxy_url = proxy_url
        self.require_token = require_token
        self.token_rotation_seconds = max(60, int(token_rotation_seconds))  # minimum 1 minute
        # Secret key for HMAC; generated at runtime. Can be overridden via env for testing.
        env_key = os.getenv("SPARKET_VALIDATOR_PUSH_SECRET")
        self._secret = env_key.encode("utf-8") if env_key else secrets.token_bytes(32)
        self._last_epoch: Optional[int] = None
        self._cached_token: Optional[str] = None

    def _current_epoch(self) -> int:
        """Get current time-based epoch."""
        return int(time.time()) // self.token_rotation_seconds

    def current_token(self) -> str:
        """Get the current valid token (time-based rotation)."""
        epoch = self._current_epoch()
        # Cache per epoch
        if self._last_epoch != epoch or not self._cached_token:
            msg = str(epoch).encode("utf-8")
            self._cached_token = hmac.new(self._secret, msg, digestmod="sha256").hexdigest()
            self._last_epoch = epoch
        return self._cached_token

    def verify_token(self, *, token: Optional[str]) -> bool:
        """Verify a token is valid for current or previous epoch."""
        if not self.require_token:
            return True
        if not token:
            return False
        # Accept current or previous epoch to allow clock drift between validator/miner
        current_epoch = self._current_epoch()
        for epoch in (current_epoch, max(0, current_epoch - 1)):
            expect = hmac.new(self._secret, str(epoch).encode("utf-8"), digestmod="sha256").hexdigest()
            if hmac.compare_digest(expect, token):
                return True
        return False

    def advertised_endpoint(self, *, axon: Any) -> Dict[str, Any]:
        if self.proxy_url:
            return {"url": self.proxy_url}
        # Prefer external_ip/external_port for broadcasting (what miners can reach)
        host = getattr(axon, "external_ip", None) or getattr(axon, "ip", None) or "127.0.0.1"
        port = getattr(axon, "external_port", None) or getattr(axon, "port", None) or 0
        return {"host": host, "port": port}


