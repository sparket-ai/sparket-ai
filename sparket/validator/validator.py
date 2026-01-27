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
from sparket.protocol.protocol import SparketSynapse
from sparket.validator.handlers.handlers import Handlers
from sparket.validator.handlers.ingest.provider_scheduler import run_provider_ingest_if_due
from sparket.validator.services import SportsDataIngestor
from sparket.validator.utils.runtime import concurrent_forward, next_backoff_delay, resolve_loop_timeouts
from sparket.validator.utils.startup import (
    check_python_requirements,
    ping_database,
    summarize_bittensor_state,
)
from sparket.validator.listeners.synapse_listener import route_incoming_synapse


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

        # Serve axon to enable external connections.
        if not self.config.neuron.axon_off:
            self.serve_axon()
        else:
            bt.logging.warning("axon off, not serving ip to chain.")

        # Create asyncio event loop to manage async tasks.
        self.loop = asyncio.get_event_loop()

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None
        self.lock = asyncio.Lock()

        # SportsDataIO ingestor (initialized during component setup)
        self.sdio_ingestor: SportsDataIngestor | None = None

        # Initialize validator components (database, etc.)
        self.initialize_components()

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
            
            # Suppress verbose SQLAlchemy logging (TRACE floods logs with SQL statements)
            import logging as std_logging
            for sqla_logger_name in ["sqlalchemy", "sqlalchemy.engine", "sqlalchemy.pool", "sqlalchemy.engine.Engine"]:
                sqla_logger = std_logging.getLogger(sqla_logger_name)
                sqla_logger.setLevel(std_logging.CRITICAL)
                sqla_logger.disabled = True
                sqla_logger.handlers = []
                sqla_logger.propagate = False
            
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

            async def _forward(synapse: SparketSynapse) -> SparketSynapse:
                try:
                    try:
                        payload = synapse.payload if hasattr(synapse, "payload") else None
                        syn_type = getattr(synapse, "type", None)
                        # Handle both enum and string types
                        if hasattr(syn_type, "value"):
                            type_val = syn_type.value
                        else:
                            type_val = syn_type  # Already a string
                        bt.logging.debug({
                            "axon_forward_received": {
                                "synapse_class": synapse.__class__.__name__,
                                "sparket_type": type_val,
                                "payload_type": type(payload).__name__ if payload is not None else None,
                            }
                        })
                    except Exception:
                        pass
                    # Route asynchronously; forward must return the synapse
                    await route_incoming_synapse(self, synapse)  # type: ignore[arg-type]
                    
                    # Debug: log synapse payload after routing
                    bt.logging.trace({
                        "forward_after_route": {
                            "payload_type": type(synapse.payload).__name__,
                            "payload_keys": list(synapse.payload.keys()) if isinstance(synapse.payload, dict) else None,
                        }
                    })
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

                    bt.logging.info(
                        {
                            "validator_loop": {
                                "iteration": loop_iter,
                                "step": self.step,
                                "block": self.block,
                            }
                        }
                    )

                    bt.logging.debug({"validator_loop": {"phase": "connection_forward_start"}})
                    try:
                        self.loop.run_until_complete(
                            asyncio.wait_for(concurrent_forward(self), timeout=timeouts["forward"])
                        )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "connection_forward_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "connection_forward_complete"}})

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

                    bt.logging.debug({"validator_loop": {"phase": "provider_ingest_start"}})
                    try:
                        self.loop.run_until_complete(
                            asyncio.wait_for(
                                run_provider_ingest_if_due(
                                    validator=self,
                                    database=self.dbm,
                                    ingestor=self.sdio_ingestor,
                                ),
                                timeout=timeouts["provider"],
                            )
                        )
                    except asyncio.TimeoutError:
                        bt.logging.warning({"validator_loop": {"phase": "provider_ingest_timeout"}})
                    bt.logging.debug({"validator_loop": {"phase": "provider_ingest_complete"}})

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
                    time.sleep(30)

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
                    time.sleep(backoff_sec)
                    backoff_sec = next_backoff_delay(backoff_sec, factor=backoff_factor, max_delay=max_backoff_sec)

        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Validator killed by keyboard interrupt.")
            exit()

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
            self.step = state["step"]
            
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
