# Clear bytecode cache FIRST, before any sparket imports load cached .pyc files
# This prevents stale code from running after updates
import shutil
import sys
from pathlib import Path

def _clear_bytecode_cache() -> int:
    """Clear __pycache__ dirs and .pyc files from sparket package."""
    base = Path(__file__).parent.parent
    removed = 0
    for cache_dir in base.rglob("__pycache__"):
        try:
            shutil.rmtree(cache_dir)
            removed += 1
        except (OSError, PermissionError):
            pass
    for pyc_file in base.rglob("*.pyc"):
        try:
            pyc_file.unlink()
        except (OSError, PermissionError):
            pass
    return removed

_CLEARED_CACHES = _clear_bytecode_cache()
if _CLEARED_CACHES > 0:
    print(f"[validator] Cleared {_CLEARED_CACHES} bytecode cache directories", file=sys.stderr, flush=True)

# Now safe to import the rest
import os
import argparse
import signal
import threading

import bittensor as bt
from dotenv import load_dotenv
from bittensor.core.config import Config as BTConfig

from sparket.validator.validator import BaseValidatorNeuron
from sparket.shared.logging import suppress_bittensor_header_warnings


class Validator(BaseValidatorNeuron):
    """
    Sparket validator entrypoint. Refer to docs/validator.md for more information.
    This class should contain minimal logic, handling just the basic startup and teardown of the validator.
    """

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)
        bt.logging.info("load_state()")
        self.load_state()
    
    # NOTE: forward() method is inherited from BaseValidatorNeuron (validator.py)
    # Do NOT override it here - broadcast logic should stay in one place


# The main function parses the configuration and runs the validator.
if __name__ == "__main__":
    import logging
    
    suppress_bittensor_header_warnings()
    bt.logging.setLevel("TRACE")
    
    # Suppress verbose SQLAlchemy logging (otherwise TRACE floods with SQL)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
    
    bt.logging.info("Starting validator")

    #change cwd to project root in case user has called entrypoint from outside the project
    os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    # print current working directory
    bt.logging.info({"current_working_directory": os.getcwd()})
    # check and log if .env file exists/is readable
    if not os.path.exists(".env"):
        bt.logging.error("Error: .env file not found")
        exit(1)
    if not os.access(".env", os.R_OK):
        bt.logging.error("Error: .env file is not readable")
        exit(1)
    bt.logging.info({"env_file_exists": os.path.exists(".env")})
    bt.logging.info({"env_file_readable": os.access(".env", os.R_OK)})

    # In test mode, PM2 sets env vars directly - don't override from .env
    test_mode = os.getenv("SPARKET_TEST_MODE", "").lower() in ("true", "1", "yes")
    if test_mode:
        bt.logging.info({"env_file_loaded": False, "reason": "test_mode_active"})
    else:
        bt.logging.info({"env_file_loaded": load_dotenv(".env", verbose=True, override=True)})

    # Compose database URL from env vars if missing or port changed
    try:
        from sparket.config.db_url import ensure_env_database_url
        result = ensure_env_database_url()
        if result.get("composed"):
            bt.logging.info({
                "db_url_env_composed": True,
                "port": result.get("port"),
                "reason": result.get("reason"),
                "old_port": result.get("old_port"),
            })
    except Exception as e:
        bt.logging.warning({"db_url_composition_error": str(e)})
    # 1) Load YAML settings and log them.
    from sparket.config.core import load_settings, sanitize_dict
    s = load_settings(role="validator")
    #bt.logging.info({"yaml_settings": sanitize_dict(s.model_dump())})

    # 2) Build a minimal parser and set defaults strictly from YAML.
    parser = argparse.ArgumentParser()
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.Axon.add_args(parser)
    BaseValidatorNeuron.add_args(parser)

    defaults = {}
    if getattr(s, "wallet", None):
        if s.wallet.name:
            defaults["wallet.name"] = s.wallet.name
        if getattr(s, "hotkey", None):
            # some bt versions nest hotkey under wallet; keep direct guard too
            pass
        if getattr(s.wallet, "hotkey", None):
            defaults["wallet.hotkey"] = s.wallet.hotkey
    if getattr(s, "chain", None) and s.chain.netuid is not None:
        defaults["netuid"] = s.chain.netuid
    endpoint = None
    if getattr(s, "subtensor", None) and s.subtensor.chain_endpoint:
        endpoint = s.subtensor.chain_endpoint
    elif getattr(s, "chain", None) and s.chain.endpoint:
        endpoint = s.chain.endpoint
    if endpoint:
        defaults["subtensor.chain_endpoint"] = endpoint
    if getattr(s, "subtensor", None) and s.subtensor.network:
        defaults["subtensor.network"] = s.subtensor.network
    # Axon external IP for broadcasting to miners
    if getattr(s, "axon", None):
        if getattr(s.axon, "external_ip", None):
            defaults["axon.external_ip"] = s.axon.external_ip
        if getattr(s.axon, "external_port", None):
            defaults["axon.external_port"] = s.axon.external_port
        if getattr(s.axon, "port", None):
            defaults["axon.port"] = s.axon.port
    parser.set_defaults(**defaults)
    bt.logging.info({"parser_defaults": defaults})

    # 3) Create bt.Config object using YAML-derived defaults (CLI can override as usual).
    cfg = BTConfig(parser, args=sys.argv[1:])

    # 4) Log the resulting bt.Config object.
    try:
        bt.logging.info({
            "bt_config": {
                "wallet": {
                    "name": getattr(cfg.wallet, "name", None),
                    "hotkey": getattr(cfg.wallet, "hotkey", None),
                },
                "netuid": getattr(cfg, "netuid", None),
                "subtensor": {
                    "network": getattr(cfg.subtensor, "network", None),
                    "chain_endpoint": getattr(cfg.subtensor, "chain_endpoint", None),
                },
            }
        })
    except Exception:
        pass

    import signal

    stop_event = threading.Event()
    received_signal: dict[str, int] = {"signum": 0}

    def _handle_signal(signum, frame):
        received_signal["signum"] = signum
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    validator = Validator(config=cfg)
    
    # Re-enable trace logging after Validator constructor 
    # (some operations during init may reset bittensor logging state)
    bt.logging.enable_trace()
    
    # Start test control API if in test mode
    test_control_api = None
    test_mode = os.getenv("SPARKET_TEST_MODE", "").lower() in ("true", "1", "yes")
    if test_mode:
        try:
            from sparket.devtools.control_api import TestControlAPI
            test_control_api = TestControlAPI(role="validator", port=8199, node=validator)
            test_control_api.start_background()
            bt.logging.info({"test_mode": "control_api_started", "port": 8199})
        except Exception as e:
            bt.logging.warning({"test_mode": "control_api_failed", "error": str(e)})
    
    try:
        validator.run()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        if test_control_api:
            try:
                test_control_api.stop()
            except Exception:
                pass
        if received_signal["signum"]:
            bt.logging.info({"validator_signal": received_signal["signum"]})
        validator.__exit__(None, None, None)
