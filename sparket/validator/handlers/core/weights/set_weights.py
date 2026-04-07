"""Handler for setting weights on chain.

Reads skill scores from the database and emits them to the chain.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import bittensor as bt
from sqlalchemy import text

from sparket.validator.config.scoring_params import get_scoring_params
from sparket.validator.scoring.determinism import get_canonical_window_bounds

from .weights_utils import (
    process_weights_for_netuid,
    convert_weights_and_uids_for_emit,
)


# SQL query to fetch latest skill scores
_SELECT_SKILL_SCORES = text(
    """
    SELECT
        m.uid,
        m.hotkey,
        mrs.skill_score
    FROM miner m
    JOIN miner_rolling_score mrs ON m.miner_id = mrs.miner_id AND m.hotkey = mrs.miner_hotkey
    WHERE m.netuid = :netuid
      AND m.active = 1
      AND mrs.as_of = :as_of
      AND mrs.window_days = :window_days
      AND mrs.skill_score IS NOT NULL
    ORDER BY m.uid
    """
)


class SetWeightsHandler:
    """Handler for setting weights on chain based on skill scores."""

    def __init__(self, database: Any):
        """Initialize the handler.

        Args:
            database: Database manager
        """
        self.database = database
        self.params = get_scoring_params()

    async def load_scores_from_db(
        self,
        netuid: int,
        n_neurons: int,
    ) -> np.ndarray:
        """Load skill scores from database for all active miners.

        Args:
            netuid: Network UID
            n_neurons: Total number of neurons in metagraph

        Returns:
            Array of scores indexed by UID (zeros for missing miners)
        """
        window_days = self.params.windows.rolling_window_days
        _, as_of = get_canonical_window_bounds(window_days)

        try:
            rows = await self.database.read(
                _SELECT_SKILL_SCORES,
                params={
                    "netuid": netuid,
                    "as_of": as_of,
                    "window_days": window_days,
                },
                mappings=True,
            )

            # If no scores for today, fall back to most recent date with scores
            if not rows or all(r["skill_score"] is None or r["skill_score"] == 0 for r in rows):
                from sqlalchemy import text as _text
                fallback_rows = await self.database.read(
                    _text("""
                        SELECT DISTINCT as_of FROM miner_rolling_score
                        WHERE window_days = :window_days
                          AND skill_score IS NOT NULL AND skill_score > 0
                        ORDER BY as_of DESC LIMIT 1
                    """),
                    params={"window_days": window_days},
                    mappings=True,
                )
                if fallback_rows:
                    fallback_as_of = fallback_rows[0]["as_of"]
                    bt.logging.info({"load_scores_fallback": {"from": str(as_of), "to": str(fallback_as_of)}})
                    rows = await self.database.read(
                        _SELECT_SKILL_SCORES,
                        params={
                            "netuid": netuid,
                            "as_of": fallback_as_of,
                            "window_days": window_days,
                        },
                        mappings=True,
                    )

            # Initialize scores array with zeros
            scores = np.zeros(n_neurons, dtype=np.float32)

            # Fill in scores from database
            for row in rows:
                uid = row["uid"]
                skill_score = row["skill_score"]

                if uid is not None and 0 <= uid < n_neurons and skill_score is not None:
                    scores[uid] = float(skill_score)

            bt.logging.info({
                "load_scores": {
                    "miners_found": len(rows),
                    "total_neurons": n_neurons,
                    "non_zero_scores": int(np.count_nonzero(scores)),
                }
            })

            return scores

        except Exception as e:
            bt.logging.warning({"load_scores_error": repr(e)})
            return np.zeros(n_neurons, dtype=np.float32)

    def set_weights(self, validator: Any) -> None:
        """Set weights on chain from in-memory scores.

        This is the synchronous interface for compatibility.
        Uses validator.scores if set, otherwise loads from database.

        Args:
            validator: Validator instance
        """
        scores = getattr(validator, "scores", None)

        if scores is None:
            bt.logging.warning("set_weights(): validator has no scores; skipping")
            return

        self._emit_weights(validator, scores)

    async def set_weights_from_db(self, validator: Any) -> None:
        """Set weights on chain from database scores.

        This is the primary method that loads skill scores from the
        database and emits them to the chain.

        Args:
            validator: Validator instance
        """
        netuid = validator.config.netuid
        n_neurons = validator.metagraph.n

        # Load scores from database
        scores = await self.load_scores_from_db(netuid, n_neurons)

        # Update validator's in-memory scores
        validator.scores = scores

        # Emit to chain
        self._emit_weights(validator, scores)

    def _get_burn_uid(self, validator: Any) -> Optional[int]:
        """Get the UID for the burn hotkey (subnet owner).

        Args:
            validator: Validator instance

        Returns:
            UID of the burn hotkey if found, None otherwise
        """
        try:
            burn_hotkey = validator.subtensor.get_subnet_owner_hotkey(
                netuid=validator.config.netuid
            )
            if burn_hotkey is None:
                bt.logging.warning("Could not retrieve subnet owner hotkey")
                return None

            hotkeys = list(validator.metagraph.hotkeys)
            if burn_hotkey not in hotkeys:
                bt.logging.warning({
                    "burn_hotkey_not_registered": {
                        "burn_hotkey": burn_hotkey,
                        "message": "Subnet owner hotkey not registered as a miner; skipping burn allocation",
                    }
                })
                return None

            burn_uid = hotkeys.index(burn_hotkey)
            bt.logging.debug({
                "burn_hotkey_found": {
                    "burn_hotkey": burn_hotkey,
                    "burn_uid": burn_uid,
                }
            })
            return burn_uid

        except Exception as e:
            bt.logging.warning({"get_burn_uid_error": str(e)})
            return None

    def _apply_burn_rate(
        self,
        raw_weights: np.ndarray,
        burn_uid: int,
        burn_rate: float,
    ) -> np.ndarray:
        """Apply burn rate redistribution to weights.

        Allocates burn_rate fraction to the burn_uid and scales down
        all other weights proportionally to maintain sum = 1.0.

        Args:
            raw_weights: Normalized weights (sum = 1.0)
            burn_uid: UID for the burn hotkey
            burn_rate: Fraction to allocate to burn hotkey (0.0 to 1.0)

        Returns:
            Adjusted weights with burn allocation applied
        """
        if burn_rate <= 0.0:
            return raw_weights

        adjusted_weights = raw_weights.copy()

        # Scale down all miner weights by (1 - burn_rate)
        adjusted_weights *= (1.0 - burn_rate)

        # Assign burn_rate to the burn_uid
        adjusted_weights[burn_uid] = burn_rate

        bt.logging.info({
            "burn_rate_applied": {
                "burn_rate": burn_rate,
                "burn_uid": burn_uid,
                "burn_weight": float(adjusted_weights[burn_uid]),
                "miner_weight_sum": float(np.sum(adjusted_weights) - adjusted_weights[burn_uid]),
            }
        })

        return adjusted_weights

    def _emit_weights(self, validator: Any, scores: np.ndarray) -> None:
        """Emit weights to chain.

        Args:
            validator: Validator instance
            scores: Score array indexed by UID
        """
        # Check if scores contains any NaN values
        if np.isnan(scores).any():
            bt.logging.warning(
                "Scores contain NaN values. Replacing with zeros."
            )
            scores = np.nan_to_num(scores, nan=0.0)

        # Calculate norm and normalize
        norm = np.linalg.norm(scores, ord=1, axis=0, keepdims=True)
        all_scores_zero = np.any(norm == 0) or np.isnan(norm).any()

        if all_scores_zero:
            # No valid miner scores - allocate 100% to burn hotkey
            burn_uid = self._get_burn_uid(validator)
            if burn_uid is None:
                bt.logging.warning(
                    "All scores are zero and burn hotkey not found; skipping weight emission"
                )
                return

            # Create weights array with 100% to burn hotkey
            raw_weights = np.zeros(len(scores), dtype=np.float32)
            raw_weights[burn_uid] = 1.0

            bt.logging.info({
                "burn_100_percent": {
                    "reason": "No valid miner scores",
                    "burn_uid": burn_uid,
                }
            })
        else:
            raw_weights = scores / norm

            # Apply burn rate if configured
            burn_rate = float(self.params.weight_emission.burn_rate)
            if burn_rate > 0.0:
                burn_uid = self._get_burn_uid(validator)
                if burn_uid is not None:
                    raw_weights = self._apply_burn_rate(raw_weights, burn_uid, burn_rate)
                else:
                    bt.logging.warning({
                        "burn_rate_skipped": {
                            "burn_rate": burn_rate,
                            "reason": "Could not find burn hotkey UID",
                        }
                    })

        bt.logging.debug({"raw_weights": raw_weights.tolist()[:10]})  # First 10
        bt.logging.debug({"raw_weight_uids": validator.metagraph.uids.tolist()[:10]})

        processed_weight_uids, processed_weights = process_weights_for_netuid(
            uids=validator.metagraph.uids,
            weights=raw_weights,
            netuid=validator.config.netuid,
            subtensor=validator.subtensor,
            metagraph=validator.metagraph,
        )

        bt.logging.debug({"processed_weights_sample": list(processed_weights[:10])})
        bt.logging.debug({"processed_weight_uids_sample": list(processed_weight_uids[:10])})

        uint_uids, uint_weights = convert_weights_and_uids_for_emit(
            uids=processed_weight_uids, weights=processed_weights
        )

        bt.logging.debug({"uint_weights_sample": list(uint_weights[:10])})
        bt.logging.debug({"uint_uids_sample": list(uint_uids[:10])})

        # Set the weights on chain
        try:
            result, msg = validator.subtensor.set_weights(
                wallet=validator.wallet,
                netuid=validator.config.netuid,
                uids=uint_uids,
                weights=uint_weights,
                wait_for_finalization=False,
                wait_for_inclusion=False,
                version_key=validator.spec_version,
            )

            if result is True:
                bt.logging.info({
                    "set_weights": "success",
                    "n_weights": len(uint_weights),
                })
            else:
                bt.logging.error({"set_weights": "failed", "message": str(msg)})

        except Exception as e:
            bt.logging.error({"set_weights_error": str(e)})


__all__ = ["SetWeightsHandler"]
