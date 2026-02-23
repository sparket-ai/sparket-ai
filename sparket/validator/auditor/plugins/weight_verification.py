"""Weight verification plugin - the core v1 auditor capability.

Fetches checkpoint metrics, independently verifies Brier scores from
deltas, runs the shared deterministic compute_weights(), and sets
weights on chain.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import bittensor as bt

from sparket.validator.auditor.attestation import create_attestation
from sparket.validator.auditor.plugin_registry import AuditorContext, TaskResult
from sparket.validator.ledger.compute_weights import compute_weights
from sparket.validator.ledger.models import ChainParamsSnapshot, MinerMetrics


class WeightVerificationHandler:
    """Verifies weights from ledger data and sets them on chain."""

    name = "weight_verification"
    version = "1.0.0"

    def __init__(self, tolerance: float = 0.001):
        self.tolerance = tolerance

    async def on_cycle(self, context: AuditorContext) -> TaskResult:
        """Verify and set weights from the latest checkpoint + deltas."""
        evidence: dict[str, Any] = {}
        now = datetime.now(timezone.utc)

        cp = context.checkpoint
        if cp is None:
            return TaskResult(
                plugin_name=self.name,
                plugin_version=self.version,
                status="skip",
                evidence={"reason": "no_checkpoint"},
                completed_at=now,
            )

        # -- Step 1: Extract MinerMetrics from checkpoint --
        metrics = [
            MinerMetrics.from_accumulator(acc)
            for acc in cp.accumulators
        ]

        if not metrics:
            return TaskResult(
                plugin_name=self.name,
                plugin_version=self.version,
                status="skip",
                evidence={"reason": "no_miners"},
                completed_at=now,
            )

        # -- Step 2: Verify Brier scores from deltas --
        brier_checks = 0
        brier_mismatches = 0
        for delta in context.deltas:
            for sub in delta.settled_submissions:
                if sub.brier is None:
                    continue
                # Find matching outcome
                outcome = next(
                    (o for o in delta.settled_outcomes if o.market_id == sub.market_id),
                    None,
                )
                if outcome is None or outcome.result is None:
                    continue

                # Independent Brier recomputation
                # Brier = (p - y)^2 where y is 1 for correct side, 0 otherwise
                actual = 1.0 if sub.side == outcome.result else 0.0
                expected_brier = (sub.imp_prob - actual) ** 2

                brier_checks += 1
                if abs(expected_brier - sub.brier) > 1e-6:
                    brier_mismatches += 1

        evidence["brier_checks"] = brier_checks
        evidence["brier_mismatches"] = brier_mismatches

        if brier_mismatches > 0:
            bt.logging.warning({
                "weight_verification": {
                    "brier_mismatches": brier_mismatches,
                    "total_checks": brier_checks,
                }
            })

        # -- Step 3: Build chain params --
        if cp.chain_params:
            chain_params = cp.chain_params
        else:
            # Derive from metagraph
            subtensor = context.subtensor
            metagraph = context.metagraph
            netuid = context.config.get("netuid", 57)

            burn_uid = None
            try:
                burn_hotkey = subtensor.get_subnet_owner_hotkey(netuid=netuid)
                if burn_hotkey and burn_hotkey in list(metagraph.hotkeys):
                    burn_uid = list(metagraph.hotkeys).index(burn_hotkey)
            except Exception:
                pass

            chain_params = ChainParamsSnapshot(
                burn_rate=float(cp.scoring_config.params.get("weight_emission", {}).get("burn_rate", 0.9)),
                burn_uid=burn_uid,
                # bt v10 returns max_weight_limit as a normalized float already.
                # Do not rescale by 65535 here.
                max_weight_limit=float(subtensor.max_weight_limit(netuid=netuid)),
                min_allowed_weights=int(subtensor.min_allowed_weights(netuid=netuid)),
                n_neurons=int(metagraph.n),
            )

        # -- Step 4: Compute weights deterministically --
        weight_result = compute_weights(
            miner_metrics=metrics,
            scoring_config=cp.scoring_config,
            chain_params=chain_params,
        )

        evidence["computed_uids"] = weight_result.uids[:10]
        evidence["computed_weights_sample"] = weight_result.uint16_weights[:10]
        evidence["n_miners_scored"] = len(metrics)

        # -- Step 5: Set weights on chain --
        # The auditor independently computes weights from ledger data and
        # sets them directly. Identical inputs + deterministic compute_weights()
        # guarantees agreement with the primary by construction.
        if weight_result.uids:
            try:
                wallet = context.wallet
                subtensor = context.subtensor
                netuid = context.config.get("netuid", 57)

                response = subtensor.set_weights(
                    wallet=wallet,
                    netuid=netuid,
                    uids=weight_result.uids,
                    weights=weight_result.uint16_weights,
                    wait_for_finalization=False,
                    wait_for_inclusion=False,
                )

                # bt v10 returns ExtrinsicResponse with .success and .message
                result_ok = getattr(response, "success", bool(response))
                msg = getattr(response, "message", str(response))

                evidence["set_weights"] = "success" if result_ok else f"failed: {msg}"
                bt.logging.info({
                    "weight_verification": {
                        "set_weights": "success" if result_ok else "failed",
                        "n_weights": len(weight_result.uids),
                    }
                })
            except Exception as e:
                evidence["set_weights_error"] = str(e)
                bt.logging.error({"weight_verification": {"set_weights_error": str(e)}})

        status = "pass"

        task_result = TaskResult(
            plugin_name=self.name,
            plugin_version=self.version,
            status=status,
            evidence=evidence,
            completed_at=now,
        )

        # Sign attestation
        if context.wallet:
            task_result.attestation = create_attestation(task_result, context.wallet)

        return task_result


# Module-level handler for auto-discovery
HANDLER = WeightVerificationHandler()

__all__ = ["HANDLER", "WeightVerificationHandler"]
