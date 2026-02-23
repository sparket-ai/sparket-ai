"""Auditor validator runtime.

Lightweight main loop: fetch ledger -> verify -> dispatch to plugins -> set weights.
No database, no SportsDataIO, no full scoring pipeline.
"""

from __future__ import annotations

import asyncio
from typing import Any

import bittensor as bt

from .plugin_registry import AuditorContext, PluginRegistry
from .sync import LedgerSync
from .verifier import ManifestVerifier


class AuditorRuntime:
    """Main auditor validator loop."""

    def __init__(
        self,
        wallet: Any,
        subtensor: Any,
        metagraph: Any,
        sync: LedgerSync,
        verifier: ManifestVerifier,
        registry: PluginRegistry,
        config: dict[str, Any] | None = None,
    ):
        self.wallet = wallet
        self.subtensor = subtensor
        self.metagraph = metagraph
        self.sync = sync
        self.verifier = verifier
        self.registry = registry
        self.config = config or {}

        self._poll_interval = int(self.config.get("auditor_poll_interval_seconds", 120))
        self._running = False

    async def run(self) -> None:
        """Main auditor loop. Runs until stopped."""
        self._running = True
        bt.logging.info({
            "auditor_runtime": {
                "status": "starting",
                "poll_interval": self._poll_interval,
                "plugins": self.registry.handlers,
            }
        })

        consecutive_errors = 0
        max_errors = 10

        while self._running:
            try:
                await self._cycle()
                consecutive_errors = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_errors += 1
                bt.logging.error({"auditor_cycle_error": str(e), "consecutive": consecutive_errors})
                if consecutive_errors >= max_errors:
                    bt.logging.error({"auditor_runtime": "too_many_errors, stopping"})
                    break
                await asyncio.sleep(min(30, 5 * consecutive_errors))
                continue

            bt.logging.info({
                "auditor_runtime": {
                    "status": "sleeping",
                    "next_poll_in_seconds": self._poll_interval,
                }
            })
            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

        self._running = False
        bt.logging.info({"auditor_runtime": "stopped"})

    def stop(self) -> None:
        """Signal the runtime to stop."""
        self._running = False

    async def _cycle(self) -> None:
        """Execute one auditor cycle."""
        bt.logging.info({"auditor_cycle": "starting"})

        # Resync metagraph
        try:
            self.metagraph.sync(subtensor=self.subtensor)
        except Exception as e:
            bt.logging.warning({"auditor_metagraph_sync_error": str(e)})

        # Fetch checkpoint + deltas
        cp, deltas = await self.sync.sync_cycle()

        if cp is None:
            bt.logging.info({"auditor_cycle": "no_checkpoint_available", "hint": "primary may not have exported yet"})
            return

        # Verify checkpoint
        cp_result = self.verifier.verify_checkpoint(cp)
        if not cp_result:
            bt.logging.error({
                "auditor_cycle": "checkpoint_verification_failed",
                "errors": cp_result.errors,
            })
            return

        # Verify deltas
        verified_deltas = []
        for delta in deltas:
            delta_result = self.verifier.verify_delta(delta)
            if delta_result:
                verified_deltas.append(delta)
            else:
                bt.logging.warning({
                    "auditor_cycle": "delta_verification_failed",
                    "errors": delta_result.errors,
                })

        # Build context for plugins
        netuid = self.config.get("netuid", 57)
        context = AuditorContext(
            checkpoint=cp,
            deltas=verified_deltas,
            accumulator_state=self.sync.accumulator,
            wallet=self.wallet,
            subtensor=self.subtensor,
            metagraph=self.metagraph,
            config={"netuid": netuid, **self.config},
        )

        # Dispatch to plugins
        results = await self.registry.dispatch(context)

        for r in results:
            bt.logging.info({
                "auditor_plugin_result": {
                    "plugin": r.plugin_name,
                    "status": r.status,
                    "evidence_keys": list(r.evidence.keys()),
                }
            })


__all__ = ["AuditorRuntime"]
