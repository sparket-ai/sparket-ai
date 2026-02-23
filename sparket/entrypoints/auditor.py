"""Auditor validator entrypoint.

Lightweight process that fetches ledger data from the primary validator,
verifies scoring integrity, and sets weights on chain.

No SportsDataIO subscription, no database, no axon serving.
"""

import asyncio
import os
import signal
import sys

import bittensor as bt
from dotenv import load_dotenv


def main() -> None:
    # Load .env if not in test mode
    if os.environ.get("SPARKET_TEST_MODE") != "true":
        load_dotenv()

    # Parse args using the v10 API (capitalised class names)
    import argparse
    parser = argparse.ArgumentParser(description="Sparket Auditor Validator")
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)
    bt.logging.add_args(parser)
    parser.add_argument("--netuid", type=int, default=57)
    parser.add_argument("--auditor.primary_hotkey", type=str, required=False)
    parser.add_argument("--auditor.primary_url", type=str, required=False)
    parser.add_argument("--auditor.poll_interval", type=int, default=900)
    parser.add_argument("--auditor.weight_tolerance", type=float, default=0.001)
    parser.add_argument("--auditor.data_dir", type=str, default="sparket/data/auditor")

    args = parser.parse_args()
    # Default to TRACE unless an explicit logging flag is provided.
    # This keeps maximum observability by default while preserving CLI overrides.
    wants_trace = bool(getattr(args, "logging.trace", False))
    wants_debug = bool(getattr(args, "logging.debug", False))
    wants_info = bool(getattr(args, "logging.info", False))
    explicit_level = wants_trace or wants_debug or wants_info

    if not explicit_level:
        bt.logging.setLevel("TRACE")
        try:
            bt.logging.enable_trace()
        except Exception:
            pass

    bt.logging.info({"auditor": "starting"})

    # Override from env vars (env takes precedence over CLI)
    primary_hotkey = os.environ.get(
        "SPARKET_AUDITOR__PRIMARY_HOTKEY",
        getattr(args, "auditor.primary_hotkey", None) or "",
    )
    primary_url = os.environ.get(
        "SPARKET_AUDITOR__PRIMARY_URL",
        getattr(args, "auditor.primary_url", None) or "",
    )
    poll_interval = int(os.environ.get(
        "SPARKET_AUDITOR__POLL_INTERVAL_SECONDS",
        getattr(args, "auditor.poll_interval", 900),
    ))
    weight_tolerance = float(os.environ.get(
        "SPARKET_AUDITOR__WEIGHT_TOLERANCE",
        getattr(args, "auditor.weight_tolerance", 0.001),
    ))
    data_dir = os.environ.get(
        "SPARKET_AUDITOR__DATA_DIR",
        getattr(args, "auditor.data_dir", "sparket/data/auditor"),
    )
    netuid = int(os.environ.get("SPARKET_CHAIN__NETUID", args.netuid or 57))

    if not primary_hotkey:
        bt.logging.error("SPARKET_AUDITOR__PRIMARY_HOTKEY is required")
        sys.exit(1)
    if not primary_url:
        bt.logging.error("SPARKET_AUDITOR__PRIMARY_URL is required")
        sys.exit(1)

    # Wallet name/hotkey from env or CLI
    wallet_name = os.environ.get("SPARKET_WALLET__NAME", getattr(args, "wallet.name", "default"))
    wallet_hotkey = os.environ.get("SPARKET_WALLET__HOTKEY", getattr(args, "wallet.hotkey", "default"))

    # Initialize bittensor components (v10 API)
    wallet = bt.Wallet(name=wallet_name, hotkey=wallet_hotkey)

    # Build subtensor config from env or CLI
    chain_endpoint = os.environ.get(
        "SPARKET_CHAIN__ENDPOINT",
        getattr(args, "subtensor.chain_endpoint", None),
    )
    network = os.environ.get(
        "SPARKET_SUBTENSOR__NETWORK",
        getattr(args, "subtensor.network", None),
    )
    if chain_endpoint:
        # Configure custom endpoint explicitly in config (v10-safe).
        subtensor_cfg = bt.Config()
        subtensor_cfg.subtensor = bt.Config()
        if network:
            subtensor_cfg.subtensor.network = network
        subtensor_cfg.subtensor.chain_endpoint = chain_endpoint
        subtensor = bt.Subtensor(config=subtensor_cfg, fallback_endpoints=[chain_endpoint])
    elif network:
        subtensor = bt.Subtensor(network=network)
    else:
        subtensor = bt.Subtensor()

    metagraph = subtensor.metagraph(netuid=netuid)

    bt.logging.info({
        "auditor_config": {
            "primary_hotkey": primary_hotkey,
            "primary_url": primary_url,
            "poll_interval": poll_interval,
            "weight_tolerance": weight_tolerance,
            "hotkey": wallet.hotkey.ss58_address,
            "netuid": netuid,
        }
    })

    # Build auditor components
    from sparket.validator.ledger.store.http_client import HTTPLedgerStore
    from sparket.validator.auditor.sync import LedgerSync
    from sparket.validator.auditor.verifier import ManifestVerifier
    from sparket.validator.auditor.plugin_registry import PluginRegistry
    from sparket.validator.auditor.runtime import AuditorRuntime
    from sparket.validator.auditor.plugins.weight_verification import WeightVerificationHandler

    store = HTTPLedgerStore(primary_url=primary_url, wallet=wallet)
    sync = LedgerSync(store=store, data_dir=data_dir)
    verifier = ManifestVerifier(primary_hotkey=primary_hotkey)

    registry = PluginRegistry()
    registry.register(WeightVerificationHandler(tolerance=weight_tolerance))

    runtime = AuditorRuntime(
        wallet=wallet,
        subtensor=subtensor,
        metagraph=metagraph,
        sync=sync,
        verifier=verifier,
        registry=registry,
        config={
            "netuid": netuid,
            "auditor_poll_interval_seconds": poll_interval,
            "auditor_weight_tolerance": weight_tolerance,
        },
    )

    # Graceful shutdown
    loop = asyncio.new_event_loop()

    def _signal_handler(sig, frame):
        bt.logging.info({"auditor": "shutdown_signal_received"})
        runtime.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        loop.run_until_complete(runtime.run())
    except KeyboardInterrupt:
        bt.logging.info({"auditor": "keyboard_interrupt"})
    finally:
        loop.run_until_complete(store.close())
        loop.close()
        bt.logging.info({"auditor": "stopped"})


if __name__ == "__main__":
    main()
