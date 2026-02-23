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
    print(f"[miner] Cleared {_CLEARED_CACHES} bytecode cache directories", file=sys.stderr, flush=True)

# Now safe to import the rest
import argparse
import asyncio
import os
import signal
import threading
import concurrent.futures

# The MIT License (MIT)
# Copyright © 2025 Sparket
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the "Software"), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import time
import typing
from typing import Optional

import bittensor as bt

# Bittensor Miner Template:


# import base miner class which takes care of most of the boilerplate
from sparket.miner.miner import BaseMinerNeuron
from sparket.miner.config.config import Config as MinerAppConfig
from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.miner.service import MinerService
from sparket.shared.logging import suppress_bittensor_header_warnings
from sparket.tools.mining_interview import ensure_interview_passed

# Optional base miner import
try:
    from sparket.miner.base import BaseMiner, BaseMinerConfig
    BASE_MINER_AVAILABLE = True
except ImportError:
    BASE_MINER_AVAILABLE = False
    BaseMiner = None  # type: ignore
    BaseMinerConfig = None  # type: ignore


def _extract_synapse_type(synapse: SparketSynapse | None) -> str | None:
    value = getattr(synapse, "type", None)
    if isinstance(value, SparketSynapseType):
        return value.value
    if isinstance(value, str):
        try:
            normalized = value.strip().lower()
        except AttributeError:
            return None
        return normalized
    return None


def _allow_unpermitted_connection_push(app_config: MinerAppConfig, synapse: SparketSynapse | None) -> bool:
    if not bool(getattr(app_config.miner, "allow_connection_info_from_unpermitted_validators", False)):
        return False
    syn_type = _extract_synapse_type(synapse)
    allow = syn_type == SparketSynapseType.CONNECTION_INFO_PUSH.value
    bt.logging.debug(
        {
            "miner_blacklist": {
                "check": "allow_connection_push",
                "synapse_type": syn_type,
                "allow": allow,
            }
        }
    )
    return allow


def _purge_pycache() -> None:
    try:
        base = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        for root, dirs, files in os.walk(base):
            for d in list(dirs):
                if d == "__pycache__":
                    cache_path = os.path.join(root, d)
                    try:
                        shutil.rmtree(cache_path, ignore_errors=True)
                    except Exception:
                        pass
    except Exception:
        pass


_purge_pycache()


class Miner(BaseMinerNeuron):
    """
    Your miner neuron class. You should use this class to define your miner's behavior. In particular, you should replace the forward function with your own logic. You may also want to override the blacklist and priority functions according to your needs.

    This class inherits from the BaseMinerNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a miner such as blacklisting unrecognized hotkeys, prioritizing requests based on stake, and forwarding requests to the forward function. If you need to define custom
    """

    def __init__(self, config=None):
        # Build a config: Env vars > YAML defaults
        if config is None:
            try:
                from sparket.config.core import load_settings
                
                cfg = BaseMinerNeuron.config()
                
                # Load YAML settings as base defaults
                settings = load_settings(role="miner")
                if getattr(settings, "wallet", None):
                    if settings.wallet.name:
                        cfg.wallet.name = settings.wallet.name
                    if getattr(settings.wallet, "hotkey", None):
                        cfg.wallet.hotkey = settings.wallet.hotkey
                if getattr(settings, "chain", None) and settings.chain.netuid is not None:
                    cfg.netuid = settings.chain.netuid
                endpoint = None
                if getattr(settings, "subtensor", None) and settings.subtensor.chain_endpoint:
                    endpoint = settings.subtensor.chain_endpoint
                elif getattr(settings, "chain", None) and settings.chain.endpoint:
                    endpoint = settings.chain.endpoint
                if endpoint:
                    cfg.subtensor.chain_endpoint = endpoint
                if getattr(settings, "subtensor", None) and settings.subtensor.network:
                    cfg.subtensor.network = settings.subtensor.network
                
                # Environment variables ALWAYS override (highest priority)
                env_wallet_name = os.getenv("SPARKET_WALLET__NAME")
                env_wallet_hotkey = os.getenv("SPARKET_WALLET__HOTKEY")
                env_axon_port = os.getenv("SPARKET_AXON__PORT")
                env_axon_host = os.getenv("SPARKET_AXON__HOST")
                
                if env_wallet_name:
                    cfg.wallet.name = env_wallet_name
                if env_wallet_hotkey:
                    cfg.wallet.hotkey = env_wallet_hotkey
                if env_axon_port:
                    cfg.axon.port = int(env_axon_port)
                    cfg.axon.external_port = int(env_axon_port)
                if env_axon_host:
                    cfg.axon.ip = env_axon_host
                    cfg.axon.external_ip = env_axon_host
                
                config = cfg
            except Exception:
                pass
        
        super(Miner, self).__init__(config=config)

        # Prevent double-logging from bittensor's standard Python logger
        # (bittensor has its own console handler, so we disable propagation
        # to prevent the root logger from also printing the same messages)
        import logging as std_logging
        bt_logger = std_logging.getLogger("bittensor")
        bt_logger.propagate = False

        self._validator_cache: dict[str, dict[str, object]] = {}
        self._validator_cache_max_entries: int = 128
        # Primary validator endpoint (set when CONNECTION_INFO_PUSH received)
        self.validator_endpoint: Optional[dict[str, object]] = None
        
        # Base miner for automatic odds generation
        self.base_miner: Optional["BaseMiner"] = None
        self._base_runtime_loop: Optional[asyncio.AbstractEventLoop] = None
        self._base_runtime_thread: Optional[threading.Thread] = None

    async def forward(self, synapse: SparketSynapse) -> SparketSynapse:
        """Handle incoming Sparket synapses.

        Currently processes connection info push messages and returns the
        synapse unchanged.
        """
        import time as _time
        start = _time.monotonic()
        syn_type = _extract_synapse_type(synapse)
        hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None)
        hotkey_short = hotkey[:16] + "..." if hotkey and len(hotkey) > 16 else hotkey
        
        bt.logging.debug({
            "miner_forward": {
                "status": "received",
                "type": syn_type,
                "from_hotkey": hotkey_short,
            }
        })
        
        try:
            if syn_type == SparketSynapseType.CONNECTION_INFO_PUSH.value and isinstance(synapse.payload, dict):
                await self._handle_connection_push(synapse)
                elapsed = _time.monotonic() - start
                bt.logging.info({
                    "miner_forward": {
                        "status": "connection_push_handled",
                        "from_hotkey": hotkey_short,
                        "elapsed_ms": round(elapsed * 1000, 1),
                    }
                })
        except Exception as exc:
            bt.logging.warning({"miner_forward_error": str(exc)})
        
        return synapse

    async def blacklist(
        self, synapse: SparketSynapse
    ) -> typing.Tuple[bool, str]:
        """Decide whether to blacklist an incoming synapse.

        Uses the configured blacklist rules plus metagraph membership and
        validator permit checks. Connection info pushes can be allowed
        even when validator permit is required.
        """
        syn_type = _extract_synapse_type(synapse)
        allow_conn_push = _allow_unpermitted_connection_push(self.app_config, synapse)

        if synapse.dendrite is None or synapse.dendrite.hotkey is None:
            bt.logging.warning(
                "Received a request without a dendrite or hotkey."
            )
            return True, "Missing dendrite or hotkey"

        hotkey = synapse.dendrite.hotkey
        hotkey_short = hotkey[:16] + "..." if len(hotkey) > 16 else hotkey

        # For CONNECTION_INFO_PUSH, always allow from registered validators
        # even if allow_connection_info_from_unpermitted_validators is False
        is_connection_push = syn_type == SparketSynapseType.CONNECTION_INFO_PUSH.value
        
        if allow_conn_push:
            bt.logging.debug({
                "miner_blacklist": {
                    "status": "allowed",
                    "reason": "connection_push_allowed_by_config",
                    "hotkey": hotkey_short,
                }
            })
            return False, "connection_info_push_allowed"

        if (
            not self.config.blacklist.allow_non_registered
            and hotkey not in self.metagraph.hotkeys
        ):
            bt.logging.warning({
                "miner_blacklist": {
                    "status": "blocked",
                    "reason": "unregistered_hotkey",
                    "hotkey": hotkey_short,
                    "synapse_type": syn_type,
                }
            })
            return True, "Unrecognized hotkey"

        uid = self.metagraph.hotkeys.index(hotkey)

        if self.config.blacklist.force_validator_permit:
            has_permit = uid < len(self.metagraph.validator_permit) and self.metagraph.validator_permit[uid]
            if not has_permit:
                # Allow CONNECTION_INFO_PUSH from registered hotkeys even without validator permit
                # This ensures miners can receive connection info during validator startup
                if is_connection_push:
                    bt.logging.debug({
                        "miner_blacklist": {
                            "status": "allowed",
                            "reason": "connection_push_from_registered",
                            "hotkey": hotkey_short,
                            "uid": uid,
                        }
                    })
                    return False, "connection_push_from_registered"
                    
                bt.logging.warning({
                    "miner_blacklist": {
                        "status": "blocked",
                        "reason": "no_validator_permit",
                        "hotkey": hotkey_short,
                        "uid": uid,
                        "synapse_type": syn_type,
                    }
                })
                return True, "Non-validator hotkey"

        bt.logging.trace(f"Not Blacklisting recognized hotkey {hotkey_short}")
        return False, "Hotkey recognized!"

    async def priority(self, synapse: SparketSynapse) -> float:
        """Assign a priority score based on caller stake."""
        if synapse.dendrite is None or synapse.dendrite.hotkey is None:
            bt.logging.warning(
                "Received a request without a dendrite or hotkey."
            )
            return 0.0

        caller_uid = self.metagraph.hotkeys.index(
            synapse.dendrite.hotkey
        )  # Get the caller index.
        priority = float(
            self.metagraph.S[caller_uid]
        )  # Return the stake as the priority.
        bt.logging.trace(
            f"Prioritizing {synapse.dendrite.hotkey} with value: {priority}"
        )
        return priority

    async def _handle_connection_push(self, synapse: SparketSynapse) -> None:
        payload = synapse.payload if isinstance(synapse.payload, dict) else {}
        host = payload.get("host")
        port = payload.get("port")
        url = payload.get("url")
        token = payload.get("token")
        hotkey = getattr(getattr(synapse, "dendrite", None), "hotkey", None)
        if hotkey is None:
            return
        try:
            from sparket.miner.database.repository import upsert_validator_endpoint
        except Exception as e:
            bt.logging.warning({"miner_repo_import_error": str(e)})
            return
        if self.dbm is None:
            bt.logging.warning("Miner database manager unavailable; cannot persist validator endpoint.")
            return
        if isinstance(port, str):
            try:
                port = int(port)
            except ValueError:
                port = None
        await upsert_validator_endpoint(
            self.dbm,
            hotkey=hotkey,
            host=host,
            port=port,
            url=url,
            token=token,
        )
        endpoint_data = {
            "hotkey": hotkey,
            "host": host,
            "port": port,
            "url": url,
            "token": token,
        }
        self._validator_cache[hotkey] = endpoint_data
        if len(self._validator_cache) > self._validator_cache_max_entries:
            # Drop the oldest cached validator to keep memory bounded.
            oldest_hotkey = next(iter(self._validator_cache))
            if oldest_hotkey != hotkey:
                self._validator_cache.pop(oldest_hotkey, None)
        # Also set the primary validator_endpoint for MinerService/ValidatorClient to use
        self.validator_endpoint = endpoint_data
        bt.logging.info({
            "miner_validator_endpoint_updated": {
                "hotkey": hotkey,
                "host": host,
                "port": port,
                "url": url,
                "token_received": bool(token),
            }
        })
        # Kick an immediate game sync when fresh validator endpoint arrives.
        if self.game_sync is not None and self._base_runtime_loop is not None:
            try:
                asyncio.run_coroutine_threadsafe(self.game_sync.sync_once(), self._base_runtime_loop)
            except Exception as exc:
                bt.logging.debug({"miner_sync_trigger_error": str(exc)})

    def initialize_base_miner(self) -> bool:
        """Initialize the base miner for automatic odds generation.
        
        The base miner is enabled by default. Set SPARKET_BASE_MINER__ENABLED=false to disable.
        
        Returns:
            True if base miner was initialized, False otherwise.
        """
        if not BASE_MINER_AVAILABLE:
            bt.logging.debug({"base_miner": "not_available"})
            return False
        
        # Load config from environment
        base_config = BaseMinerConfig.from_env()
        
        if not base_config.enabled:
            bt.logging.debug({"base_miner": "disabled"})
            return False
        
        if self.validator_client is None:
            bt.logging.warning({"base_miner": "validator_client_unavailable"})
            return False
        
        if self.game_sync is None:
            bt.logging.warning({"base_miner": "game_sync_unavailable"})
            return False
        
        try:
            hotkey = self.wallet.hotkey.ss58_address
            # Token getter for base miner authentication
            def _get_token() -> Optional[str]:
                endpoint = getattr(self, "validator_endpoint", None) or {}
                return endpoint.get("token") if isinstance(endpoint, dict) else None
            
            self.base_miner = BaseMiner(
                hotkey=hotkey,
                config=base_config,
                validator_client=self.validator_client,
                game_sync=self.game_sync,
                get_token=_get_token,
            )
            bt.logging.info({
                "base_miner": "initialized",
                "odds_api_key": "configured" if base_config.odds_api_key else "not_configured",
                "market_blend_weight": base_config.market_blend_weight,
                "odds_refresh_seconds": base_config.odds_refresh_seconds,
            })
            return True
        except Exception as e:
            bt.logging.warning({"base_miner": "init_failed", "error": str(e)})
            return False
    
    async def start_base_miner(self) -> None:
        """Start the base miner background loops."""
        if self.base_miner is not None:
            await self.base_miner.start()
    
    async def stop_base_miner(self) -> None:
        """Stop the base miner."""
        if self.base_miner is not None:
            await self.base_miner.stop()

    def start_base_runtime(self) -> bool:
        """Run game sync + base miner in a persistent async thread."""
        if self._base_runtime_thread and self._base_runtime_thread.is_alive():
            return True
        if self.base_miner is None:
            return False
        loop = asyncio.new_event_loop()
        self._base_runtime_loop = loop

        async def _bootstrap() -> None:
            if self.game_sync is not None:
                await self.game_sync.start()
                # Prime local market cache immediately for first odds cycle.
                await self.game_sync.sync_once()
            await self.base_miner.start()

        def _runner() -> None:
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_bootstrap())
                loop.run_forever()
            finally:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()

        thread = threading.Thread(target=_runner, daemon=True, name="base-miner-runtime")
        thread.start()
        self._base_runtime_thread = thread
        return True

    def stop_base_runtime(self) -> None:
        """Stop base miner runtime thread and async services."""
        loop = self._base_runtime_loop
        thread = self._base_runtime_thread
        if loop is None or thread is None:
            return

        async def _shutdown() -> None:
            if self.base_miner is not None:
                await self.base_miner.stop()
            if self.game_sync is not None:
                await self.game_sync.stop()

        try:
            fut = asyncio.run_coroutine_threadsafe(_shutdown(), loop)
            fut.result(timeout=10)
        except (TimeoutError, concurrent.futures.TimeoutError, Exception):
            pass
        try:
            loop.call_soon_threadsafe(loop.stop)
        except Exception:
            pass
        try:
            thread.join(timeout=5)
        except Exception:
            pass
        self._base_runtime_loop = None
        self._base_runtime_thread = None


# This is the main function, which runs the miner.
async def _log_validator_endpoints(miner: Miner) -> None:
    if miner.dbm is None:
        return
    try:
        from sparket.miner.database.repository import list_validator_endpoints
        rows = await list_validator_endpoints(miner.dbm)
        bt.logging.info({
            "miner_validator_endpoints": [
                {
                    "hotkey": row.hotkey,
                    "host": row.host,
                    "port": row.port,
                    "url": row.url,
                    "last_seen": row.last_seen.isoformat() if row.last_seen else None,
                }
                for row in rows
            ]
        })
    except Exception as exc:
        bt.logging.warning({"miner_log_endpoints_error": str(exc)})


async def _start_base_miner_async(miner: Miner) -> None:
    """Start base miner in async context."""
    if miner.base_miner is not None:
        await miner.base_miner.start()


async def _stop_base_miner_async(miner: Miner) -> None:
    """Stop base miner in async context."""
    if miner.base_miner is not None:
        await miner.base_miner.stop()


if __name__ == "__main__":
    suppress_bittensor_header_warnings()
    bt.logging.setLevel("TRACE")
    bt.logging.info("Starting miner")
    bt.logging.info({"current_working_directory": os.getcwd()})

    stop_event = threading.Event()

    received_signal: dict[str, int] = {"signum": 0}

    def _handle_signal(signum, frame):
        received_signal["signum"] = signum
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    miner = Miner()
    
    # Initialize base miner (enabled by default; disable with SPARKET_BASE_MINER__ENABLED=false)
    # This provides automatic odds generation using ESPN data + optional The-Odds-API
    base_miner_initialized = False
    base_gate_ok = True
    if BASE_MINER_AVAILABLE:
        try:
            candidate_cfg = BaseMinerConfig.from_env() if BaseMinerConfig is not None else None
            if candidate_cfg is not None and candidate_cfg.enabled:
                base_gate_ok, gate_reason = ensure_interview_passed()
                if not base_gate_ok:
                    bt.logging.error(
                        {
                            "base_miner_gate": "blocked",
                            "reason": gate_reason,
                            "interview": "Sparket mining interview",
                        }
                    )
                    bt.logging.error(
                        "Base miner disabled until interview pass. Run: python -m sparket.tools.mining_interview"
                    )
        except Exception as e:
            base_gate_ok = False
            bt.logging.error({"base_miner_gate": "error", "error": str(e)})

    if base_gate_ok:
        base_miner_initialized = miner.initialize_base_miner()
    if base_miner_initialized:
        try:
            started = miner.start_base_runtime()
            bt.logging.info({"base_miner": "started" if started else "not_started"})
        except Exception as e:
            bt.logging.warning({"base_miner": "start_failed", "error": str(e)})
    
    # Start control API for external model integration
    # Configure via SPARKET_MINER_API_PORT (default: 8198) 
    # Disable with SPARKET_MINER_API_ENABLED=false
    control_api = None
    api_enabled = os.getenv("SPARKET_MINER_API_ENABLED", "true").lower() not in ("false", "0", "no")
    api_port = int(os.getenv("SPARKET_MINER_API_PORT", "8198"))
    
    if api_enabled:
        try:
            from sparket.devtools.control_api import TestControlAPI
            control_api = TestControlAPI(role="miner", port=api_port, node=miner)
            control_api.start_background()
            bt.logging.info({"miner_api": "started", "port": api_port, "endpoints": [
                "POST /action/submit-odds",
                "POST /action/submit-outcome",
                "POST /action/fetch-games",
                "GET /games",
                "GET /health",
            ]})
        except Exception as e:
            bt.logging.warning({"miner_api": "failed_to_start", "error": str(e)})
    
    try:
        miner.run()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        # Stop base miner
        try:
            miner.stop_base_runtime()
            bt.logging.info({"base_miner": "stopped"})
        except Exception:
            pass
        
        if control_api:
            try:
                control_api.stop()
            except Exception:
                pass
        if received_signal["signum"]:
            bt.logging.info({"miner_signal": received_signal["signum"]})
