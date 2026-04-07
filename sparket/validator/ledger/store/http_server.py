"""Authenticated HTTP endpoint serving ledger data to auditors.

Runs as an async task in the primary validator's event loop. Routes:
  POST /ledger/auth/challenge - request auth challenge
  POST /ledger/auth/respond   - submit signed challenge for bearer token
  GET  /ledger/checkpoints/latest - fetch latest checkpoint
  GET  /ledger/deltas?epoch=N&since=X - list delta IDs
  GET  /ledger/deltas/{id}    - fetch a specific delta
  POST /ledger/recompute      - trigger epoch bump (primary control only)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import bittensor as bt
from aiohttp import web

from sparket.validator.ledger.auth import AccessPolicy
from sparket.validator.ledger.store.filesystem import FilesystemStore


def _hk(hotkey: str | None) -> str:
    """Truncate hotkey for log readability."""
    if not hotkey:
        return "none"
    return hotkey[:16]


class LedgerHTTPServer:
    """Lightweight async HTTP server for ledger distribution."""

    def __init__(
        self,
        store: FilesystemStore,
        access_policy: AccessPolicy,
        exporter: Any = None,
        host: str = "0.0.0.0",
        port: int = 8200,
    ):
        self.store = store
        self.access_policy = access_policy
        self.exporter = exporter
        self.host = host
        self.port = port
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None

    def _build_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post("/ledger/auth/challenge", self._handle_challenge)
        app.router.add_post("/ledger/auth/respond", self._handle_respond)
        app.router.add_get("/ledger/checkpoints/latest", self._handle_latest_checkpoint)
        app.router.add_get("/ledger/deltas", self._handle_list_deltas)
        app.router.add_get("/ledger/deltas/{delta_id}", self._handle_get_delta)
        app.router.add_post("/ledger/recompute", self._handle_recompute)
        # Prometheus metrics endpoint (no auth - operational data only)
        app.router.add_get("/metrics", self._handle_metrics)
        return app

    async def start(self) -> None:
        """Start the HTTP server."""
        self._app = self._build_app()
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        bt.logging.info({"ledger_http": {"status": "started", "port": self.port}})

    async def stop(self) -> None:
        """Stop the HTTP server."""
        if self._runner:
            await self._runner.cleanup()
            bt.logging.info({"ledger_http": "stopped"})

    # -- Auth routes --

    async def _handle_challenge(self, request: web.Request) -> web.Response:
        """Issue a challenge nonce for a hotkey."""
        try:
            body = await request.json()
            hotkey = body.get("hotkey", "")
        except Exception:
            bt.logging.warning({"ledger_request": {"endpoint": "auth/challenge", "status": 400, "error": "invalid_body"}})
            return web.json_response({"error": "invalid_body"}, status=400)

        result = self.access_policy.check_eligibility(hotkey)
        if not result.eligible:
            bt.logging.info({"ledger_request": {"endpoint": "auth/challenge", "hotkey": _hk(hotkey), "status": 403, "reason": result.reason}})
            return web.json_response(
                {"error": "ineligible", "reason": result.reason}, status=403,
            )

        nonce = self.access_policy.issue_challenge(hotkey)
        bt.logging.info({"ledger_request": {"endpoint": "auth/challenge", "hotkey": _hk(hotkey), "status": 200}})
        return web.json_response({"nonce": nonce})

    async def _handle_respond(self, request: web.Request) -> web.Response:
        """Verify signed challenge and issue bearer token."""
        try:
            body = await request.json()
            hotkey = body.get("hotkey", "")
            nonce = body.get("nonce", "")
            signature = body.get("signature", "")
        except Exception:
            bt.logging.warning({"ledger_request": {"endpoint": "auth/respond", "status": 400, "error": "invalid_body"}})
            return web.json_response({"error": "invalid_body"}, status=400)

        token = self.access_policy.verify_response(hotkey, nonce, signature)
        if token is None:
            bt.logging.warning({"ledger_request": {"endpoint": "auth/respond", "hotkey": _hk(hotkey), "status": 403}})
            return web.json_response({"error": "auth_failed"}, status=403)

        bt.logging.info({"ledger_request": {"endpoint": "auth/respond", "hotkey": _hk(hotkey), "status": 200}})
        return web.json_response({"token": token})

    # -- Auth middleware --

    def _check_auth(self, request: web.Request) -> str | None:
        """Validate bearer token from Authorization header. Returns hotkey or None."""
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return None
        token = auth[7:]
        return self.access_policy.validate_token(token)

    # -- Data routes --

    async def _handle_latest_checkpoint(self, request: web.Request) -> web.Response:
        hotkey = self._check_auth(request)
        if hotkey is None:
            bt.logging.debug({"ledger_request": {"endpoint": "checkpoints/latest", "status": 401}})
            return web.json_response({"error": "unauthorized"}, status=401)

        if not self.access_policy.check_rate_limit(hotkey):
            bt.logging.warning({"ledger_request": {"endpoint": "checkpoints/latest", "hotkey": _hk(hotkey), "status": 429}})
            return web.json_response({"error": "rate_limited"}, status=429)

        cp = await self.store.get_latest_checkpoint()
        if cp is None:
            bt.logging.info({"ledger_request": {"endpoint": "checkpoints/latest", "hotkey": _hk(hotkey), "status": 404}})
            return web.json_response({"error": "no_checkpoint"}, status=404)

        data = cp.model_dump(mode="json")
        bt.logging.info({"ledger_request": {"endpoint": "checkpoints/latest", "hotkey": _hk(hotkey), "status": 200, "miners": len(cp.accumulators)}})
        return web.json_response(data)

    async def _handle_list_deltas(self, request: web.Request) -> web.Response:
        hotkey = self._check_auth(request)
        if hotkey is None:
            bt.logging.debug({"ledger_request": {"endpoint": "deltas", "status": 401}})
            return web.json_response({"error": "unauthorized"}, status=401)

        if not self.access_policy.check_rate_limit(hotkey):
            bt.logging.warning({"ledger_request": {"endpoint": "deltas", "hotkey": _hk(hotkey), "status": 429}})
            return web.json_response({"error": "rate_limited"}, status=429)

        try:
            epoch = int(request.query.get("epoch", "0"))
        except ValueError:
            return web.json_response({"error": "invalid_epoch"}, status=400)

        since_str = request.query.get("since")
        since = None
        if since_str:
            try:
                # URL query parsers decode '+' as space; restore it so
                # timezone offsets like '+00:00' parse correctly.
                since = datetime.fromisoformat(since_str.replace(" ", "+"))
            except ValueError:
                return web.json_response({"error": "invalid_since"}, status=400)

        delta_ids = await self.store.list_deltas(epoch, since)
        bt.logging.info({"ledger_request": {"endpoint": "deltas", "hotkey": _hk(hotkey), "status": 200, "count": len(delta_ids)}})
        return web.json_response({"deltas": delta_ids, "epoch": epoch})

    async def _handle_get_delta(self, request: web.Request) -> web.Response:
        hotkey = self._check_auth(request)
        if hotkey is None:
            bt.logging.debug({"ledger_request": {"endpoint": "deltas/{id}", "status": 401}})
            return web.json_response({"error": "unauthorized"}, status=401)

        if not self.access_policy.check_rate_limit(hotkey):
            bt.logging.warning({"ledger_request": {"endpoint": "deltas/{id}", "hotkey": _hk(hotkey), "status": 429}})
            return web.json_response({"error": "rate_limited"}, status=429)

        delta_id = request.match_info["delta_id"]
        delta = await self.store.get_delta(delta_id)
        if delta is None:
            bt.logging.info({"ledger_request": {"endpoint": "deltas/{id}", "hotkey": _hk(hotkey), "status": 404, "delta_id": delta_id}})
            return web.json_response({"error": "not_found"}, status=404)

        data = delta.model_dump(mode="json")
        bt.logging.info({"ledger_request": {"endpoint": "deltas/{id}", "hotkey": _hk(hotkey), "status": 200, "delta_id": delta_id}})
        return web.json_response(data)

    async def _handle_recompute(self, request: web.Request) -> web.Response:
        """Trigger epoch bump (primary control only - no auth token, local only)."""
        # Only allow from localhost
        peer = request.remote
        if peer not in ("127.0.0.1", "::1", "localhost"):
            bt.logging.warning({"ledger_request": {"endpoint": "recompute", "status": 403, "peer": peer}})
            return web.json_response({"error": "forbidden"}, status=403)

        if self.exporter is None:
            return web.json_response({"error": "exporter_not_configured"}, status=500)

        try:
            body = await request.json()
            reason_code = body["reason_code"]
            reason_detail = body["reason_detail"]
            affected_event_ids = body.get("affected_event_ids", [])
            severity = body.get("severity", "correction")
        except (KeyError, json.JSONDecodeError) as e:
            return web.json_response({"error": f"invalid_body: {e}"}, status=400)

        cp = await self.exporter.bump_epoch(
            reason_code=reason_code,
            reason_detail=reason_detail,
            affected_event_ids=affected_event_ids,
            severity=severity,
        )

        bt.logging.info({"ledger_request": {"endpoint": "recompute", "status": 200, "epoch": cp.manifest.checkpoint_epoch}})
        return web.json_response({
            "epoch": cp.manifest.checkpoint_epoch,
            "status": "ok",
        })


    # -- Prometheus metrics --

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Serve Prometheus metrics in text exposition format. No auth required."""
        from prometheus_client import generate_latest
        from sparket.validator.observability.metrics import REGISTRY

        body = generate_latest(REGISTRY)
        return web.Response(
            body=body,
            content_type="text/plain; version=0.0.4; charset=utf-8",
        )


__all__ = ["LedgerHTTPServer"]
