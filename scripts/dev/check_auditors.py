"""Check auditor validator weight health from chain state.

Queries the metagraph and reports:
- Which validators have validator_permit
- When each last set weights (block number)
- Cosine similarity between primary and each auditor weight vector
- Flags stale or drifting auditors

Usage:
    uv run python scripts/dev/check_auditors.py --netuid 57
    uv run python scripts/dev/check_auditors.py --netuid 57 --primary-uid 0 --stale-blocks 1000
"""

from __future__ import annotations

import argparse
import sys

import bittensor as bt
import numpy as np


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two weight vectors."""
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def main() -> None:
    parser = argparse.ArgumentParser(description="Check auditor validator weight health")
    bt.Subtensor.add_args(parser)
    parser.add_argument("--netuid", type=int, default=57, help="Subnet UID")
    parser.add_argument("--primary-uid", type=int, default=None, help="Primary validator UID (auto-detected if omitted)")
    parser.add_argument("--primary-hotkey", type=str, default=None, help="Primary validator hotkey (for auto-detection)")
    parser.add_argument("--stale-blocks", type=int, default=1000, help="Flag validators that haven't set weights in this many blocks")
    parser.add_argument("--drift-threshold", type=float, default=0.01, help="Flag if cosine distance exceeds this")

    config = bt.config(parser)
    netuid = config.netuid or 57
    stale_blocks = config.stale_blocks
    drift_threshold = config.drift_threshold

    print(f"Connecting to subtensor (netuid={netuid})...")
    subtensor = bt.Subtensor(config=config)
    metagraph = subtensor.metagraph(netuid=netuid)
    current_block = subtensor.block

    print(f"\nValidator Weights Check (block {current_block:,})")
    print(f"{'=' * 72}")

    # Find validators with vpermit
    validators = []
    for uid in range(metagraph.n):
        try:
            vpermit = bool(metagraph.validator_permit[uid])
        except (IndexError, AttributeError):
            continue
        if vpermit:
            validators.append(uid)

    if not validators:
        print("No validators with validator_permit found.")
        sys.exit(0)

    # Detect primary UID
    primary_uid = config.primary_uid
    if primary_uid is None and config.primary_hotkey:
        hotkeys = list(metagraph.hotkeys)
        if config.primary_hotkey in hotkeys:
            primary_uid = hotkeys.index(config.primary_hotkey)
    if primary_uid is None:
        # Default to the first validator (highest stake usually)
        stakes = [(uid, float(metagraph.S[uid])) for uid in validators]
        stakes.sort(key=lambda x: x[1], reverse=True)
        primary_uid = stakes[0][0]
        print(f"Auto-detected primary: UID {primary_uid} (highest stake)")

    # Get primary weight vector
    try:
        primary_weights = metagraph.W[primary_uid].numpy() if hasattr(metagraph.W[primary_uid], "numpy") else np.array(metagraph.W[primary_uid])
    except Exception:
        primary_weights = np.zeros(metagraph.n)

    primary_has_weights = np.any(primary_weights > 0)
    if not primary_has_weights:
        print(f"WARNING: Primary UID {primary_uid} has no weights set on chain.\n")

    # Get last weight-set block for each validator
    try:
        last_update = metagraph.last_update
    except AttributeError:
        last_update = None

    # Print table
    print(f"\n{'UID':>4}  {'Hotkey':<16}  {'Stake':>12}  {'Last Update':>12}  {'Cos Sim':>8}  {'Status':<20}")
    print(f"{'-' * 4}  {'-' * 16}  {'-' * 12}  {'-' * 12}  {'-' * 8}  {'-' * 20}")

    for uid in validators:
        hotkey = metagraph.hotkeys[uid][:16]
        stake = float(metagraph.S[uid])

        # Last weight update
        if last_update is not None:
            try:
                last_block = int(last_update[uid])
                blocks_ago = current_block - last_block
                last_str = f"{blocks_ago:,} ago"
            except (IndexError, TypeError):
                last_block = 0
                blocks_ago = float("inf")
                last_str = "never"
        else:
            last_block = 0
            blocks_ago = float("inf")
            last_str = "unknown"

        # Cosine similarity
        try:
            uid_weights = metagraph.W[uid].numpy() if hasattr(metagraph.W[uid], "numpy") else np.array(metagraph.W[uid])
            has_weights = np.any(uid_weights > 0)
        except Exception:
            uid_weights = np.zeros(metagraph.n)
            has_weights = False

        if uid == primary_uid:
            cos_str = "---"
            status = "PRIMARY"
        elif not has_weights:
            cos_str = "n/a"
            status = "NO_WEIGHTS"
        elif not primary_has_weights:
            cos_str = "n/a"
            status = "PRIMARY_EMPTY"
        else:
            cos_sim = cosine_similarity(primary_weights, uid_weights)
            cos_str = f"{cos_sim:.4f}"
            cos_distance = 1.0 - cos_sim

            if cos_distance > drift_threshold:
                status = f"DRIFT (>{drift_threshold})"
            elif blocks_ago > stale_blocks:
                status = f"STALE (>{stale_blocks:,})"
            else:
                status = "OK"

        # Override status for stale validators (even if weights match)
        if uid != primary_uid and has_weights and blocks_ago > stale_blocks:
            status = f"STALE (>{stale_blocks:,})"

        print(f"{uid:>4}  {hotkey:<16}  {stake:>12,.0f}  {last_str:>12}  {cos_str:>8}  {status:<20}")

    # Summary
    print(f"\n{'=' * 72}")
    print(f"Total validators with vpermit: {len(validators)}")
    print(f"Primary UID: {primary_uid}")
    print(f"Stale threshold: {stale_blocks:,} blocks")
    print(f"Drift threshold: {drift_threshold}")


if __name__ == "__main__":
    main()