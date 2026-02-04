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


import copy
import os
import time
import numpy as np
import asyncio
import argparse
import threading
import bittensor as bt

from typing import List, Union
from traceback import print_exception

from sparket.base.neuron import BaseNeuron
from sparket.devtools.mock_bittensor import MockDendrite
from sparket.base.config import add_validator_args
from sparket.validator.config.config import Config as ValidatorAppConfig
from sparket.config import sanitize_dict
from sparket.protocol.protocol import SparketSynapse, SparketSynapseType
from sparket.validator.comms import ValidatorComms
from sparket.validator.handlers.handlers import Handlers
from sparket.validator.services import SportsDataIngestor
from sparket.validator.utils.runtime import next_backoff_delay, resolve_loop_timeouts
from sparket.validator.utils.startup import (
    check_python_requirements,
    ping_database,
    summarize_bittensor_state,
)
from sparket.validator.listeners.synapse_listener import route_incoming_synapse
from sparket.shared.log_colors import LogColors


class BaseValidatorNeuron(BaseNeuron):
    """
    Base class for Bittensor validators. Your validator should inherit from this class.
    """

    neuron_type: str = "ValidatorNeuron"

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_validator_args(cls, parser)

    def __init__(self, config=None):
        bt.logging.info("Validator __init__")
        bt.logging.info(f"Config: {config}")
        super().__init__(config=config)
        # Save a copy of the hotkeys to local memory.
        self.hotkeys = copy.deepcopy(self.metagraph.hotkeys)

        # Dendrite lets us send messages to other nodes (axons) in the network.
        if self.config.mock:
            self.dendrite = MockDendrite(wallet=self.wallet)
        else:
            self.dendrite = bt.Dendrite(wallet=self.wallet)
        bt.logging.info(f"Dendrite: {self.dendrite}")
        try:
            bt.logging.info({
                "validator_dendrite": {
                    "mock": bool(self.config.mock),
                    "wallet_hotkey": getattr(getattr(self.wallet, "hotkey", None), "ss58_address", None),
                    "object": repr(self.dendrite),
                }
            })
        except Exception:
            pass

        # Set up initial scoring weights for validation
        bt.logging.info("Building validation weights.")
        self.scores = np.zeros(self.metagraph.n, dtype=np.float32)

        # Init sync with the network. Updates the metagraph.
        self.sync()

        # Create asyncio event loop early - needed for database and security_manager init
        self.loop = asyncio.get_event_loop()

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None
        self.lock = asyncio.Lock()

        # SportsDataIO ingestor (initialized during component setup)
        self.sdio_ingestor: SportsDataIngestor | None = None
        
        # Background SDIO ingest task management
        self._sdio_ingest_running: bool = False
        self._sdio_ingest_task: asyncio.Task | None = None

        # Initialize validator components (database, security_manager, etc.)
        # MUST happen before serve_axon() so security middleware can be injected
        self.initialize_components()

        # Serve axon to enable external connections.
        # Security middleware is injected here using the security_manager we just created
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning("axon off, not serving ip to chain.")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.should_exit = True
        except Exception:
            pass

        try:
            thread = getattr(self, "thread", None)
            if thread and thread.is_alive():
                thread.join(timeout=5.0)
        except Exception as e:
            bt.logging.warning({"validator_context": {"thread_join_error": str(e)}})

        try:
            axon = getattr(self, "axon", None)
            if axon is not None:
                axon.stop()
        except Exception as e:
            bt.logging.warning({"validator_context": {"axon_stop_error": str(e)}})

        # Close the main dendrite session
        try:
            dendrite = getattr(self, "dendrite", None)
            if dendrite is not None:
                loop = getattr(self, "loop", None)
                if loop and not loop.is_closed():
                    loop.run_until_complete(dendrite.aclose())
        except Exception as e:
            bt.logging.warning({"validator_context": {"dendrite_close_error": str(e)}})

        try:
            dbm = getattr(self, "dbm", None)
            loop = getattr(self, "loop", None)
            if dbm is not None and loop and not loop.is_closed():
                loop.run_until_complete(dbm.engine.dispose())
        except Exception as e:
            bt.logging.warning({"validator_context": {"dbm_dispose_error": str(e)}})

        try:
            manager = getattr(self, "scoring_worker_manager", None)
            if manager is not None:
                manager.shutdown()
        except Exception as e:
            bt.logging.warning({"validator_context": {"worker_shutdown_error": str(e)}})

        # Stop SDIO background ingest task
        try:
            self._sdio_ingest_running = False
            task = getattr(self, "_sdio_ingest_task", None)
            if task is not None and not task.done():
                task.cancel()
                try:
                    loop = getattr(self, "loop", None)
                    if loop and not loop.is_closed():
                        loop.run_until_complete(task)
                except asyncio.CancelledError:
                    pass
        except Exception as e:
            bt.logging.warning({"validator_context": {"sdio_task_cancel_error": str(e)}})

        # Cancel any running broadcast task and close its dendrite
        try:
            broadcaster = getattr(self, "_broadcaster", None)
            if broadcaster is not None:
                broadcaster.cancel()
                loop = getattr(self, "loop", None)
                if loop and not loop.is_closed():
                    loop.run_until_complete(broadcaster.close())
        except Exception as e:
            bt.logging.warning({"validator_context": {"broadcaster_cancel_error": str(e)}})

        try:
            ingestor = getattr(self, "sdio_ingestor", None)
            loop = getattr(self, "loop", None)
            if ingestor is not None and loop and not loop.is_closed():
                loop.run_until_complete(ingestor.close())
        except Exception as e:
            bt.logging.warning({"validator_context": {"sdio_ingestor_close_error": str(e)}})

        try:
            self.save_state()
        except Exception as e:
            bt.logging.warning({"validator_context": {"save_state_error": str(e)}})

        return False



    def initialize_components(self):
        """Initialize core validator components.

        This keeps the neuron entrypoint minimal while centralizing setup.
        Local imports avoid circular dependencies and heavy import cost when unused.
        """
        try:
            # Configure logging FIRST (suppress noise, add colored labels, prevent double-logging)
            from sparket.validator.utils.logging_config import configure_validator_logging
            configure_validator_logging()
            
            bt.logging.info({"validator_init": {"step": "importing_modules"}})
            from sparket.validator.database.init import initialize as init_db
            from sparket.validator.database.dbm import DBM

            # Load application config (env + optional YAML overrides)
            bt.logging.info({"validator_init": {"step": "loading_app_config"}})
            self.app_config: ValidatorAppConfig = ValidatorAppConfig()

            # Log sanitized effective config at startup
            try:
                core = getattr(self.app_config, "core", None)
                serialized = core.model_dump() if core is not None else self.app_config.model_dump()
                bt.logging.info({"validator_config": sanitize_dict(serialized)})
            except Exception as e:
                bt.logging.warning({"validator_init": {"config_serialization_error": str(e)}})

            bt.logging.info("Initializing validator components (database, handlers, etc.)")

            # Log current environment variables for debugging (sanitized)
            try:
                env_vars = {
                    k: v
                    for k, v in os.environ.items()
                    if k.startswith("SPARKET_") or k.startswith("DATABASE_") or k.startswith("BT_") or k.startswith("BITTENSOR_")
                }
                bt.logging.info({"environment_variables": sanitize_dict(env_vars)})
            except Exception:
                pass

            # Compose database URL if missing using discrete fields (fallback guard)
            bt.logging.info({"validator_init": {"step": "compose_database_url"}})
            try:
                from sparket.config.db_url import ensure_config_database_url
                core = getattr(self.app_config, "core", None)
                core_db = getattr(core, "database", None)
                result = ensure_config_database_url(core_db)
                if result.get("composed"):
                    composed = getattr(core_db, "url", None) if core_db else None
                    bt.logging.info({"db_url_composed": True, "url_redacted": sanitize_dict({"url": composed}).get("url", "***")})
                else:
                    bt.logging.info({"db_url_composed": False, "url_already_set": result.get("url_already_set")})
            except Exception as e:
                bt.logging.warning({"validator_init": {"compose_url_error": str(e)}})

            # Initialize database
            bt.logging.info({"validator_init": {"step": "initializing_database"}})
            init_db(self.app_config)
            bt.logging.info({"validator_init": {"step": "database_initialized"}})
            
            bt.logging.info({"validator_init": {"step": "creating_dbm"}})
            self.dbm = DBM.get_manager(self.app_config)
            bt.logging.info({"validator_init": {"step": "dbm_created"}})

            try:
                self.sdio_ingestor = SportsDataIngestor(database=self.dbm)
                bt.logging.info({"validator_init": {"step": "sdio_ingestor_initialized"}})
                # Initialize from DB to avoid re-fetching on restart
                restored = self.loop.run_until_complete(self.sdio_ingestor.initialize_from_db())
                bt.logging.info({"validator_init": {"step": "sdio_ingestor_restored_from_db", "events": restored}})
                # Start background ingest task (runs independently of main loop)
                self._start_sdio_ingest_background()
                bt.logging.info({"validator_init": {"step": "sdio_background_ingest_started"}})
            except Exception as e:
                self.sdio_ingestor = None
                bt.logging.warning({"validator_init": {"sdio_ingestor_init_error": str(e)}})

            # Environment checks (best-effort)
            try:
                bt.logging.info({"validator_init": {"step": "checking_python_requirements"}})
                check_python_requirements()
            except Exception as e:
                bt.logging.warning({"validator_init": {"python_requirements_check_error": str(e)}})

            # Database ping (fail-fast)
            bt.logging.info({"validator_init": {"step": "pinging_database"}})
            try:
                ok = self.loop.run_until_complete(ping_database(self.dbm))
                if not ok:
                    raise RuntimeError("database ping failed after init")
                bt.logging.info({"validator_init": {"database_ping": "success"}})
            except Exception as e:
                bt.logging.error({"validator_startup_db_error": str(e)})
                raise

            # Initialize handlers
            bt.logging.info({"validator_init": {"step": "initializing_handlers"}})
            self.handlers = Handlers(self.dbm)
            bt.logging.info({"validator_init": {"step": "handlers_initialized"}})

            # Initialize ValidatorComms for token management
            bt.logging.info({"validator_init": {"step": "initializing_comms"}})
            proxy_url = getattr(getattr(self.app_config, "validator", None), "proxy_url", None)
            require_token = bool(getattr(getattr(self.app_config, "validator", None), "require_push_token", True))
            step_rotation = int(getattr(getattr(self.app_config, "validator", None), "token_rotation_steps", 10))
            self.comms = ValidatorComms(
                proxy_url=proxy_url,
                require_token=require_token,
                step_rotation=step_rotation,
            )
            bt.logging.info({
                "validator_init": {
                    "step": "comms_initialized",
                    "require_token": require_token,
                    "step_rotation": step_rotation,
                }
            })

            # Track last connection push time
            self._last_connection_push = 0.0

            # Initialize SecurityManager for rate limiting and blacklisting
            bt.logging.info({"validator_init": {"step": "initializing_security_manager"}})
            try:
                from sparket.validator.security import SecurityManager
                self.security_manager = SecurityManager(database=self.dbm)
                # Load permanent blacklist from database
                self.loop.run_until_complete(self.security_manager.load_blacklist_from_db())
                # Initialize with current metagraph hotkeys
                self.security_manager.update_registered_hotkeys(self.metagraph)
                bt.logging.info({
                    "validator_init": {
                        "step": "security_manager_initialized",
                        "stats": self.security_manager.get_stats(),
                    }
                })
            except Exception as e:
                bt.logging.warning({"validator_init": {"security_manager_error": str(e)}})
                self.security_manager = None

            # Initialize broadcaster for non-blocking connection pushes
            bt.logging.info({"validator_init": {"step": "initializing_broadcaster"}})
            try:
                from sparket.validator.broadcaster import ConnectionBroadcaster
                self._broadcaster = ConnectionBroadcaster(
                    wallet=self.wallet,
                    max_concurrent=50,  # Max simultaneous connections
                    timeout_per_miner=20.0,  # Individual timeout per miner
                )
                bt.logging.info({"validator_init": {"step": "broadcaster_initialized"}})
            except Exception as e:
                bt.logging.warning({"validator_init": {"broadcaster_error": str(e)}})
                self._broadcaster = None

            # Start scoring worker if enabled
            self.scoring_worker_manager = None
            validator_cfg = getattr(self.app_config, "validator", None)
            worker_enabled = bool(getattr(validator_cfg, "scoring_worker_enabled", False))
            worker_count = int(getattr(validator_cfg, "scoring_worker_count", 1))
            self.scoring_worker_fallback = bool(getattr(validator_cfg, "scoring_worker_fallback", True))
            core_runtime = getattr(getattr(self.app_config, "core", None), "runtime", None)
            if core_runtime is not None and getattr(core_runtime, "test_mode", False):
                worker_enabled = False
            self.scoring_worker_enabled = worker_enabled
            if worker_enabled:
                try:
                    from sparket.validator.scoring.worker.manager import ScoringWorkerManager
                    self.scoring_worker_manager = ScoringWorkerManager(
                        self.app_config,
                        self.dbm,
                        worker_count=worker_count,
                    )
                    self.scoring_worker_manager.start()
                    bt.logging.info({"scoring_worker": "started"})
                except Exception as e:
                    bt.logging.warning({"scoring_worker": "failed_to_start", "error": str(e)})
            else:
                bt.logging.info({"scoring_worker": "disabled"})

            # Summarize bittensor state for visibility
            try:
                bt.logging.info({"validator_init": {"step": "summarizing_bittensor_state"}})
                summarize_bittensor_state(self)
            except Exception as e:
                bt.logging.warning({"validator_init": {"summarize_state_error": str(e)}})
            
            bt.logging.info({"validator_init": {"step": "components_initialized"}})

            interval = getattr(getattr(self.app_config, "validator", None), "connection_push_interval_seconds", 300)
            self.connection_push_interval = max(int(interval), 1)
            bt.logging.info({"validator_init": {"step": "connection_push_interval_seconds", "value": self.connection_push_interval}})

            # Recover state from database
            bt.logging.info({"validator_init": {"step": "recovering_state_from_db"}})
            try:
                self.loop.run_until_complete(self._recover_state_from_db())
                bt.logging.info({"validator_init": {"step": "state_recovered"}})
            except Exception as e:
                bt.logging.warning({"validator_init": {"state_recovery_error": str(e)}})

        except Exception as e:
            bt.logging.error(f"Validator component initialization failed: {e}")
            import traceback
            bt.logging.debug({"validator_init": {"traceback": traceback.format_exc()}})
            raise
    def serve_axon(self):
        """Create and serve the validator axon, routing incoming synapses to listeners."""
        try:
            # Initialize axon and attach forward to route incoming synapses
            cfg = self.config() if callable(self.config) else self.config
            self.axon = bt.Axon(
                wallet=self.wallet,
                config=cfg,
            )
            # Force external_ip from YAML settings since Bittensor config may not propagate
            try:
                from sparket.config import load_settings
                settings = load_settings(role="validator")
                axon_cfg = getattr(settings, "axon", None)
                if axon_cfg:
                    ext_ip = getattr(axon_cfg, "external_ip", None)
                    ext_port = getattr(axon_cfg, "external_port", None)
                    if ext_ip and ext_ip != "0.0.0.0":
                        self.axon.external_ip = ext_ip
                        bt.logging.info({"axon_external_ip_set": ext_ip})
                    if ext_port:
                        self.axon.external_port = ext_port
            except Exception as e:
                bt.logging.warning({"axon_external_ip_error": str(e)})

            # Inject security middleware for early request rejection
            # Must happen AFTER axon creation but BEFORE attach/start
            if hasattr(self, "security_manager") and self.security_manager is not None:
                try:
                    from sparket.validator.security import inject_security_middleware
                    inject_security_middleware(self.axon, self.security_manager)
                except Exception as e:
                    bt.logging.warning({"security_middleware_injection_error": str(e)})

            async def _forward(synapse: SparketSynapse) -> SparketSynapse:
                try:
                    # Minimal logging to reduce overhead during high traffic
                    syn_type = getattr(synapse, "type", None)
                    type_val = syn_type.value if hasattr(syn_type, "value") else syn_type
                    
                    # Only log at TRACE level to avoid flooding during high traffic
                    bt.logging.trace({
                        "axon_forward": {
                            "type": type_val,
                        }
                    })
                    
                    # Route asynchronously; forward must return the synapse
                    await route_incoming_synapse(self, synapse)  # type: ignore[arg-type]
                except Exception as e:
                    bt.logging.warning({"forward_route_error": str(e)})
                return synapse

            self.axon.attach(forward_fn=_forward)
            bt.logging.info(f"Axon created: {self.axon}")
            registered = False
            def _is_loopback(addr: str | None) -> bool:
                if not addr:
                    return False
                lower = addr.lower()
                return "127.0.0.1" in lower or "localhost" in lower
            try:
                self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)
                registered = True
            except Exception as serve_err:
                config_subtensor = getattr(self.config, "subtensor", None)
                desired_endpoint = getattr(config_subtensor, "chain_endpoint", None) if config_subtensor else None
                host = None
                if desired_endpoint and isinstance(desired_endpoint, str):
                    try:
                        from urllib.parse import urlparse
                        parsed = urlparse(desired_endpoint if "://" in desired_endpoint else f"tcp://{desired_endpoint}")
                        host = parsed.hostname
                    except Exception:
                        host = None
                config_axon = getattr(self.config, "axon", None)
                axon_host = getattr(config_axon, "ip", None) if config_axon else None
                is_local_intent = any([
                    _is_loopback(host),
                    _is_loopback(axon_host),
                    isinstance(getattr(config_subtensor, "network", None), str)
                    and getattr(config_subtensor, "network", "").lower() == "local",
                ])
                message = str(serve_err)
                if is_local_intent and "InvalidIpAddress" in message:
                    bt.logging.warning({
                        "axon_registration_skipped": {
                            "reason": "InvalidIpAddress",
                            "endpoint": desired_endpoint,
                            "axon_ip": axon_host,
                            "detail": message,
                        }
                    })
                else:
                    raise
            try:
                axon_info = {
                    "wallet_hotkey": getattr(getattr(self.wallet, "hotkey", None), "ss58_address", None),
                    "netuid": self.config.netuid,
                    "config_chain_endpoint": getattr(getattr(self.config, "subtensor", None), "chain_endpoint", None),
                    "ip": getattr(self.axon, "ip", None),
                    "port": getattr(self.axon, "port", None),
                    "external_ip": getattr(self.axon, "external_ip", None),
                    "external_port": getattr(self.axon, "external_port", None),
                }
                bt.logging.info({"axon_binding": axon_info})
                
            except Exception:
                pass
            self.axon.start()
            if not registered:
                bt.logging.info({"axon_registration": "skipped_local_override"})
        except Exception as e:
            bt.logging.warning({"validator_serve_axon_error": str(e)})

    async def forward(self, synapse: bt.Synapse) -> bt.Synapse:
        """Handle incoming synapse requests.
        
        This satisfies the abstract method from BaseNeuron.
        Actual routing is handled by the function attached via axon.attach().
        """
        # The axon's attached _forward function handles incoming requests.
        # This method exists to satisfy the ABC requirement.
        # If called directly, route through the same logic.
        from sparket.validator.listeners.synapse_listener import route_incoming_synapse
        try:
            await route_incoming_synapse(self, synapse)  # type: ignore[arg-type]
        except Exception as e:
            bt.logging.warning({"forward_direct_error": str(e)})
        return synapse

    async def _push_connection_info(self) -> None:
        """Push connection info (including auth token) to miners periodically.
        
        Miners use this information to submit odds/outcomes back to the validator.
        This is called from the main loop on each iteration, but only broadcasts
        when the configured interval has elapsed.
        
        Uses fire-and-forget pattern with background batched broadcasting to avoid
        blocking the main loop while still tracking delivery stats.
        """
        import time as _time
        
        # Check if enough time has passed since last push
        now = _time.time()
        interval = getattr(self, "connection_push_interval", 300)
        last_push = getattr(self, "_last_connection_push", 0.0)
        
        if now - last_push < interval:
            return
        
        # Ensure comms is initialized
        if not hasattr(self, "comms") or self.comms is None:
            bt.logging.debug({"forward": "comms_not_ready"})
            return
        
        # Ensure axon is available
        if not hasattr(self, "axon") or self.axon is None:
            bt.logging.debug({"forward": "axon_not_ready"})
            return
        
        # Ensure broadcaster is initialized
        if not hasattr(self, "_broadcaster") or self._broadcaster is None:
            bt.logging.debug({"forward": "broadcaster_not_ready"})
            return
        
        try:
            # Build endpoint info
            endpoint = self.comms.advertised_endpoint(axon=self.axon)
            
            # Get current token
            step = int(self.step) if hasattr(self.step, '__int__') else 0
            token = self.comms.current_token(step=step)
            
            # Build payload
            payload = {
                **endpoint,
                "token": token,
                "hotkey": self.wallet.hotkey.ss58_address,
                "step": step,
            }
            
            # Create synapse
            synapse = SparketSynapse(
                type=SparketSynapseType.CONNECTION_INFO_PUSH,
                payload=payload,
            )
            
            # Get miner axons (exclude self, filter active)
            try:
                axons = [
                    ax for i, ax in enumerate(self.metagraph.axons)
                    if getattr(ax, "port", 0) > 0
                    and self.metagraph.hotkeys[i] != self.wallet.hotkey.ss58_address
                ]
            except Exception:
                axons = []
            
            if not axons:
                bt.logging.debug({"forward": "no_miner_axons"})
                return
            
            # Log stats from PREVIOUS broadcast (if available)
            last_stats = self._broadcaster.get_last_stats()
            if last_stats:
                bt.logging.debug({
                    "previous_broadcast_stats": last_stats,
                })
            
            # Fire-and-forget: start broadcast in background
            # This returns immediately without waiting for responses
            started = self._broadcaster.start_broadcast(axons, synapse)
            
            if started:
                bt.logging.debug({
                    "broadcast_started": {
                        "total_axons": len(axons),
                        "fire_and_forget": True,
                    }
                })
            
            # Update timestamp regardless of whether broadcast started
            # (if skipped due to already running, we don't want to spam retries)
            self._last_connection_push = now
            
        except Exception as e:
            bt.logging.warning({"connection_push_error": str(e)})

    # -------------------------------------------------------------------------
    # Background SDIO Ingest
    # -------------------------------------------------------------------------
    
    def _start_sdio_ingest_background(self) -> None:
        """Start the SDIO ingest as a background task.
        
        Runs independently of the main loop to avoid blocking.
        Uses its own cadence and error handling.
        """
        if self.sdio_ingestor is None:
            bt.logging.warning({"sdio_background": "ingestor_not_available"})
            return
        
        if self._sdio_ingest_running:
            bt.logging.debug({"sdio_background": "already_running"})
            return
        
        self._sdio_ingest_running = True
        self._sdio_ingest_task = asyncio.ensure_future(
            self._sdio_ingest_loop(),
            loop=self.loop,
        )
        bt.logging.info({"sdio_background": "started"})
    
    async def _sdio_ingest_loop(self) -> None:
        """Background loop for SDIO provider ingestion.
        
        Runs continuously with configurable interval, independent of the main
        validator loop. This prevents long ingestion operations from blocking
        critical path operations like scoring and weight setting.
        """
        from datetime import datetime, timezone
        
        # Get interval from config (default 60 seconds)
        interval_seconds = 60
        try:
            timers = getattr(getattr(self.app_config, "core", None), "timers", None)
            if timers is not None:
                interval_seconds = max(30, int(getattr(timers, "sdio_ingest_interval_seconds", 60)))
        except Exception:
            pass
        
        bt.logging.info({
            "sdio_background_loop": {
                "interval_seconds": interval_seconds,
                "status": "starting",
            }
        })
        
        consecutive_errors = 0
        max_consecutive_errors = 5
        error_backoff = 30  # Additional wait on errors
        
        while self._sdio_ingest_running:
            cycle_start = datetime.now(timezone.utc)
            try:
                if self.sdio_ingestor is not None:
                    await self.sdio_ingestor.run_once(now=cycle_start)
                    
                    # Also run provider closing upserts
                    from sparket.validator.handlers.ingest.provider_ingest import upsert_provider_closing
                    from sparket.providers.providers import get_provider_id
                    
                    provider_id = get_provider_id("SDIO")
                    if provider_id:
                        try:
                            upserts = await upsert_provider_closing(
                                database=self.dbm,
                                provider_id=provider_id,
                                close_ts=cycle_start,
                            )
                            if upserts:
                                bt.logging.debug({"sdio_background_closing_upserts": upserts})
                        except Exception as exc:
                            bt.logging.warning({"sdio_background_closing_error": str(exc)})
                
                consecutive_errors = 0  # Reset on success
                
            except asyncio.CancelledError:
                bt.logging.info({"sdio_background_loop": "cancelled"})
                break
            except Exception as e:
                consecutive_errors += 1
                bt.logging.warning({
                    "sdio_background_error": str(e),
                    "consecutive_errors": consecutive_errors,
                })
                
                if consecutive_errors >= max_consecutive_errors:
                    bt.logging.error({
                        "sdio_background_loop": "too_many_errors",
                        "stopping": True,
                    })
                    break
                
                # Extra backoff on error
                await asyncio.sleep(error_backoff)
            
            # Calculate time taken and sleep for remainder of interval
            elapsed = (datetime.now(timezone.utc) - cycle_start).total_seconds()
            sleep_time = max(1, interval_seconds - elapsed)
            
            bt.logging.debug({
                "sdio_background_cycle": {
                    "elapsed_seconds": round(elapsed, 1),
                    "sleep_seconds": round(sleep_time, 1),
                }
            })
            
            try:
                await asyncio.sleep(sleep_time)
            except asyncio.CancelledError:
                break
        
        self._sdio_ingest_running = False
        bt.logging.info({"sdio_background_loop": "stopped"})

    def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Continuously forwards queries to the miners on the network, rewarding their responses and updating the scores accordingly.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The essence of the validator's operations is in the forward function, which is called every step. The forward function is responsible for querying the network and scoring the responses.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        # Check that validator is registered on the network.
        self.sync()

        bt.logging.info(f"Validator starting at block: {self.block}")

        # This loop maintains the validator's operations until intentionally stopped.
        try:
            loop_iter = 0
            failure_count = 0
            backoff_sec = 5.0
            backoff_factor = 2.0
            max_backoff_sec = 300.0
            max_failures = 5

            while True:
                loop_iter += 1
                try:
                    timers = getattr(getattr(self.app_config, "core", None), "timers", None)
                    step_target = getattr(timers, "step_target_seconds", 12) if timers is not None else 12
                    timeouts = resolve_loop_timeouts(step_target)

                    # Heartbeat log - always visible even during traffic flood
                    bt.logging.info(
                        {
                            "validator_loop": {
                                "iteration": loop_iter,
                                "step": self.step,
                                "block": self.block,
                                "heartbeat": True,
                            }
                        }
                    )

                    bt.logging.debug({"validator_loop": {"phase": "connection_push_start"}})
                    try:
                        self.loop.run_until_complete(
                            asyncio.wait_for(self._push_connection_info(), timeout=timeouts["forward"])
                        )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "connection_push_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "connection_push_complete"}})

                    from sparket.validator.handlers.score.main_score import (
                        run_main_scoring_if_due_async,
                        run_snapshot_pipeline_if_due_async,
                    )
                    from sparket.validator.scoring.worker.scheduler import (
                        schedule_scoring_work_if_due_async,
                    )

                    manager = getattr(self, "scoring_worker_manager", None)
                    worker_healthy = False
                    if manager is not None:
                        manager.monitor()
                        try:
                            fresh = self.loop.run_until_complete(
                                asyncio.wait_for(
                                    manager.check_heartbeat(),
                                    timeout=timeouts["worker_heartbeat"],
                                )
                            )
                            worker_healthy = bool(fresh)
                            if not fresh:
                                bt.logging.warning({"scoring_worker": "heartbeat_stale"})
                        except asyncio.TimeoutError:
                            bt.logging.warning({"scoring_worker": "heartbeat_timeout"})

                    bt.logging.debug({"validator_loop": {"phase": "scoring_start"}})
                    try:
                        if getattr(self, "scoring_worker_enabled", False):
                            fallback_enabled = bool(getattr(self, "scoring_worker_fallback", True))
                            if fallback_enabled and not worker_healthy:
                                self.loop.run_until_complete(
                                    asyncio.wait_for(
                                        run_main_scoring_if_due_async(
                                            step=self.step,
                                            app_config=self.app_config,
                                            handlers=self.handlers,
                                            validator=self,
                                            emit_weights=False,  # Base neuron handles weight cadence
                                        ),
                                        timeout=timeouts["scoring"],
                                    )
                                )
                            else:
                                self.loop.run_until_complete(
                                    asyncio.wait_for(
                                        run_snapshot_pipeline_if_due_async(
                                            step=self.step,
                                            app_config=self.app_config,
                                            handlers=self.handlers,
                                        ),
                                        timeout=timeouts["scoring"],
                                    )
                                )
                                self.loop.run_until_complete(
                                    asyncio.wait_for(
                                        schedule_scoring_work_if_due_async(
                                            step=self.step,
                                            app_config=self.app_config,
                                            database=self.dbm,
                                        ),
                                        timeout=timeouts["scoring"],
                                    )
                                )
                        else:
                            self.loop.run_until_complete(
                                asyncio.wait_for(
                                    run_main_scoring_if_due_async(
                                        step=self.step,
                                        app_config=self.app_config,
                                        handlers=self.handlers,
                                        validator=self,
                                        emit_weights=False,  # Base neuron handles weight cadence
                                    ),
                                    timeout=timeouts["scoring"],
                                )
                            )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "scoring_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "scoring_complete"}})

                    # NOTE: Provider ingest (SDIO) now runs in background task
                    # See _sdio_ingest_loop() - started during initialize_components()

                    from sparket.validator.handlers.ingest.outcome_processor import run_outcome_processing_if_due

                    bt.logging.debug({"validator_loop": {"phase": "outcome_process_start"}})
                    try:
                        self.loop.run_until_complete(
                            asyncio.wait_for(
                                run_outcome_processing_if_due(
                                    validator=self,
                                    database=self.dbm,
                                ),
                                timeout=timeouts["outcome"],
                            )
                        )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "outcome_process_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "outcome_process_complete"}})

                    from sparket.validator.handlers.maintenance.cleanup import run_cleanup_if_due

                    bt.logging.debug({"validator_loop": {"phase": "cleanup_start"}})
                    try:
                        self.loop.run_until_complete(
                            asyncio.wait_for(
                                run_cleanup_if_due(validator=self, database=self.dbm),
                                timeout=timeouts["cleanup"],
                            )
                        )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "cleanup_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "cleanup_complete"}})

                    bt.logging.debug({"validator_loop": {"phase": "sync_start"}})
                    self.sync()
                    # Sync miners to database after metagraph sync
                    try:
                        self.loop.run_until_complete(
                            self.handlers.sync_metagraph_handler.run_async(self, set_weights=False)
                        )
                    except Exception as e:
                        bt.logging.warning({"miner_db_sync_error": str(e)})
                    bt.logging.debug({"validator_loop": {"phase": "sync_complete"}})

                    self.step += 1
                    # Use async sleep to allow event loop to process other tasks
                    self.loop.run_until_complete(asyncio.sleep(30))

                    failure_count = 0
                    backoff_sec = 5.0
                except KeyboardInterrupt:
                    raise
                except Exception as err:
                    failure_count += 1
                    bt.logging.error(f"Error during validation: {str(err)}")
                    bt.logging.debug(
                        str(print_exception(type(err), err, err.__traceback__))
                    )
                    if failure_count > max_failures:
                        bt.logging.error({"validator_restart": "max_failures_exceeded"})
                        raise
                    bt.logging.warning({
                        "validator_restart": {
                            "attempt": failure_count,
                            "sleep_seconds": backoff_sec,
                        }
                    })
                    # Use async sleep to allow event loop to process other tasks
                    self.loop.run_until_complete(asyncio.sleep(backoff_sec))
                    backoff_sec = next_backoff_delay(backoff_sec, factor=backoff_factor, max_delay=max_backoff_sec)

        except KeyboardInterrupt:
            self._cleanup()
            bt.logging.success("Validator killed by keyboard interrupt.")
            exit()
    
    def _cleanup(self):
        """Clean up resources on shutdown."""
        try:
            if hasattr(self, 'axon') and self.axon:
                self.axon.stop()
        except Exception:
            pass
        
        # Close the main dendrite session
        try:
            if hasattr(self, 'dendrite') and self.dendrite:
                # Run aclose in the event loop
                if hasattr(self, 'loop') and self.loop and not self.loop.is_closed():
                    self.loop.run_until_complete(self.dendrite.aclose())
        except Exception:
            pass

    def set_weights(self):
        """Set weights on chain using scores from database."""
        try:
            if not hasattr(self, 'handlers') or not hasattr(self.handlers, 'set_weights_handler'):
                bt.logging.debug({"validator_set_weights": "skipped", "reason": "handler_not_ready"})
                return

            # Run async weight emission in the event loop
            self.loop.run_until_complete(
                self.handlers.set_weights_handler.set_weights_from_db(self)
            )
        except Exception as e:
            bt.logging.warning({"validator_set_weights_error": str(e)})



    async def _recover_state_from_db(self) -> None:
        """Recover scoring state from database after restart.
        
        This ensures the validator can resume from the last known good state
        stored in the database, mitigating issues from restarts due to the
        known bittensor memory leak or other crashes.
        """
        try:
            # Load latest skill scores from database
            if hasattr(self, 'handlers') and hasattr(self.handlers, 'set_weights_handler'):
                scores = await self.handlers.set_weights_handler.load_scores_from_db(
                    netuid=self.config.netuid,
                    n_neurons=self.metagraph.n,
                )
                
                if np.any(scores > 0):
                    self.scores = scores
                    non_zero = int(np.count_nonzero(scores))
                    bt.logging.info({
                        "db_state_recovered": {
                            "scores_loaded": True,
                            "non_zero_scores": non_zero,
                            "total_neurons": len(scores),
                        }
                    })
                else:
                    bt.logging.info({"db_state_recovered": {"scores_loaded": False, "reason": "no_scores_in_db"}})
            
            # Reset stuck scoring jobs
            await self._reset_stale_scoring_jobs()
            
        except Exception as e:
            bt.logging.warning({"db_state_recovery_error": str(e)})

    async def _reset_stale_scoring_jobs(self) -> None:
        """Reset scoring jobs stuck in 'running' state from previous crashes."""
        try:
            from sqlalchemy import text
            from datetime import datetime, timezone, timedelta
            
            # Find jobs stuck in running state for > 2 hours
            stuck_threshold = datetime.now(timezone.utc) - timedelta(hours=2)
            
            result = await self.dbm.write(
                text(
                    """
                    UPDATE scoring_job_state
                    SET status = 'pending',
                        last_error = 'Reset on validator startup'
                    WHERE status = 'running'
                      AND started_at < :threshold
                    RETURNING job_id
                    """
                ),
                params={"threshold": stuck_threshold},
                return_rows=True,
            )
            
            if result:
                job_ids = [r[0] for r in result]
                bt.logging.info({"reset_stuck_jobs": {"count": len(job_ids), "jobs": job_ids}})
                
        except Exception as e:
            bt.logging.warning({"reset_stuck_jobs_error": str(e)})

    def save_state(self):
        """Saves the state of the validator to a file.
        
        Primary state is in the database. This saves minimal in-memory state
        as a backup for quick recovery on simple restarts.
        """
        bt.logging.info("Saving validator state.")

        try:
            # Save the state of the validator to file.
            np.savez(
                self.config.neuron.full_path + "/state.npz",
                step=self.step,
                scores=self.scores,
                hotkeys=self.hotkeys,
            )
        except Exception as e:
            bt.logging.warning({"save_state_error": str(e)})

    def load_state(self):
        """Loads the state of the validator from a file.
        
        Note: DB state recovery in _recover_state_from_db is preferred as it
        has more recent data. This is a fallback for file-based state.
        """
        bt.logging.info("Loading validator state from file.")
        
        try:
            state_path = self.config.neuron.full_path + "/state.npz"
            if not os.path.exists(state_path):
                bt.logging.info({"load_state": "no_state_file_found"})
                return
            
            state = np.load(state_path)
            # Ensure step is an integer, not numpy array
            loaded_step = state["step"]
            self.step = int(loaded_step) if hasattr(loaded_step, '__int__') else 0
            
            # Only load file-based scores if we don't have DB scores
            if np.all(self.scores == 0):
                file_scores = state["scores"]
                if len(file_scores) == len(self.scores):
                    self.scores = file_scores
                    bt.logging.info({"load_state": "scores_loaded_from_file"})
            
            self.hotkeys = state["hotkeys"]
            bt.logging.info({"load_state": "success", "step": self.step})
            
        except Exception as e:
            bt.logging.warning({"load_state_error": str(e)})
