"""HTTP-based LedgerStore client for auditor validators.

Handles challenge-response authentication automatically, caches bearer
tokens, and provides incremental delta sync support.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import bittensor as bt
import httpx

from sparket.validator.ledger.models import CheckpointWindow, DeltaWindow


class HTTPLedgerStore:
    """Auditor-side client for fetching ledger data from the primary."""

    def __init__(
        self,
        primary_url: str,
        wallet: Any,
        timeout: float = 30.0,
        max_retries: int = 3,
    ):
        self.primary_url = primary_url.rstrip("/")
        self.wallet = wallet
        self._hotkey = wallet.hotkey.ss58_address
        self._token: str | None = None
        self._token_expires: float = 0.0
        self._client = httpx.AsyncClient(timeout=timeout)
        self._max_retries = max_retries

    async def close(self) -> None:
        await self._client.aclose()

    # -- Auth --

    async def _ensure_auth(self) -> str:
        """Ensure we have a valid bearer token, refreshing if needed."""
        if self._token and time.time() < self._token_expires - 60:
            return self._token

        # Step 1: Request challenge
        resp = await self._client.post(
            f"{self.primary_url}/ledger/auth/challenge",
            json={"hotkey": self._hotkey},
        )
        if resp.status_code != 200:
            raise ConnectionError(f"Auth challenge failed: {resp.status_code} {resp.text}")

        nonce = resp.json()["nonce"]

        # Step 2: Sign nonce
        signature = self.wallet.hotkey.sign(nonce.encode())
        sig_hex = signature.hex() if isinstance(signature, bytes) else str(signature)

        # Step 3: Submit response
        resp = await self._client.post(
            f"{self.primary_url}/ledger/auth/respond",
            json={"hotkey": self._hotkey, "nonce": nonce, "signature": sig_hex},
        )
        if resp.status_code != 200:
            raise ConnectionError(f"Auth respond failed: {resp.status_code} {resp.text}")

        self._token = resp.json()["token"]
        self._token_expires = time.time() + 3500  # ~1 hour minus buffer
        return self._token

    def _auth_headers(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}

    async def _get(self, path: str) -> httpx.Response:
        """Authenticated GET with retry."""
        for attempt in range(self._max_retries):
            try:
                token = await self._ensure_auth()
                resp = await self._client.get(
                    f"{self.primary_url}{path}",
                    headers=self._auth_headers(token),
                )
                if resp.status_code == 401:
                    self._token = None  # Force re-auth
                    continue
                return resp
            except httpx.TransportError as e:
                if attempt == self._max_retries - 1:
                    raise
                wait = 2 ** attempt
                bt.logging.warning({"ledger_http_client": {"retry": attempt, "wait": wait, "error": str(e)}})
                import asyncio
                await asyncio.sleep(wait)
        raise ConnectionError("Max retries exceeded")

    # -- LedgerStore interface --

    async def put_checkpoint(self, cp: CheckpointWindow) -> str:
        raise NotImplementedError("Client is read-only")

    async def put_delta(self, delta: DeltaWindow) -> str:
        raise NotImplementedError("Client is read-only")

    async def get_latest_checkpoint(self) -> CheckpointWindow | None:
        resp = await self._get("/ledger/checkpoints/latest")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return CheckpointWindow(**resp.json())

    async def list_deltas(
        self, epoch: int, since: datetime | None = None,
    ) -> list[str]:
        from urllib.parse import urlencode
        qs: dict[str, str] = {"epoch": str(epoch)}
        if since:
            qs["since"] = since.isoformat()
        resp = await self._get(f"/ledger/deltas?{urlencode(qs)}")
        resp.raise_for_status()
        return resp.json().get("deltas", [])

    async def get_delta(self, delta_id: str) -> DeltaWindow | None:
        resp = await self._get(f"/ledger/deltas/{delta_id}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return DeltaWindow(**resp.json())


__all__ = ["HTTPLedgerStore"]
