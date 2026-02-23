# The MIT License (MIT)
# Copyright © 2023 Yuma Rao

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import os
import time
import asyncio
import threading
import argparse
import traceback
from contextlib import contextmanager
from time import monotonic

import bittensor as bt

from sparket.base.neuron import BaseNeuron
from sparket.base.config import add_miner_args

from typing import Any, Awaitable, Callable, Tuple, TypeVar, Union

from sparket.miner.config.config import Config as MinerAppConfig
from sparket.miner.database import DBM, initialize
from sparket.miner.utils import (
    check_python_requirements,
    ping_database,
    summarize_miner_state,
)
from sparket.protocol.protocol import SparketSynapse  # Required for axon.attach type resolution

T = TypeVar("T")
METAGRAPH_SYNC_COOLDOWN_SECONDS = 120


def _run_coroutine(factory: Callable[[], Awaitable[T]]) -> T:
    try:
        return asyncio.run(factory())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(factory())
        finally:
            asyncio.set_event_loop(None)
            loop.close()


class BaseMinerNeuron(BaseNeuron):
    """
    Base class for Bittensor miners.
    """

    neuron_type: str = "MinerNeuron"

    @classmethod
    def add_args(cls, parser: argparse.ArgumentParser):
        super().add_args(parser)
        add_miner_args(cls, parser)

    def __init__(self, config=None):
        super().__init__(config=config)

        start_time = monotonic()
        bt.logging.info({"miner_init": {"phase": "start"}})

        # Load application config (env + optional YAML overrides)
        self.app_config: MinerAppConfig = MinerAppConfig()
        self._log_app_config()
        self._log_relevant_env()

        # Use project-relative data directory based on test mode
        test_mode = getattr(getattr(self.app_config, "core", None), "test_mode", False)
        db_path = self.app_config.miner.database_path(test_mode)

        # Initialize database schema and manager
        bt.logging.info({"miner_init": {"step": "initializing_database", "db_path": db_path, "test_mode": test_mode}})
        try:
            initialize(self.app_config)
        except Exception as exc:
            bt.logging.error({"miner_db_init_error": str(exc)})
            bt.logging.error({"miner_init": {"step": "initializing_database_failed", "error": str(exc)}})
        else:
            bt.logging.info({"miner_init": {"step": "database_initialized", "db_path": db_path}})

        self.dbm: DBM | None = None
        bt.logging.info(
            {
                "miner_init": {
                    "step": "creating_db_manager",
                    "db_url": f"sqlite+aiosqlite:///{db_path}",
                    "test_mode": test_mode,
                }
            }
        )
        try:
            self.dbm = DBM(self.app_config)
        except Exception as exc:
            bt.logging.error({"miner_db_manager_error": str(exc)})
            bt.logging.error({"miner_init": {"step": "db_manager_creation_failed", "error": str(exc)}})
        else:
            bt.logging.info({"miner_init": {"step": "db_manager_ready"}})
            if self.dbm is not None:
                bt.logging.info({"miner_init": {"step": "pinging_database"}})
                try:
                    ok = _run_coroutine(lambda: ping_database(self.dbm))
                    if not ok:
                        bt.logging.error({"miner_init": {"step": "database_ping_failed"}})
                except Exception as exc:  # pragma: no cover - diagnostics
                    bt.logging.error({"miner_init": {"step": "database_ping_exception", "error": str(exc)}})
        
        # Initialize validator client and game sync for test mode control
        self.validator_client = None
        self.game_sync = None
        try:
            from sparket.miner.client import ValidatorClient
            from sparket.miner.sync import GameDataSync
            
            self.validator_client = ValidatorClient(
                wallet=self.wallet,
                metagraph=self.metagraph,
            )
            
            if self.dbm is not None:
                self.game_sync = GameDataSync(
                    database=self.dbm,
                    client=self.validator_client,
                    odds_window_days=self.app_config.validator.ingest.odds_window_days,
                )
            bt.logging.info({"miner_init": {"step": "validator_client_ready"}})
        except Exception as exc:
            bt.logging.warning({"miner_init": {"validator_client_error": str(exc)}})

        bt.logging.info({"miner_init": {"step": "checking_dependencies"}})
        try:
            check_python_requirements()
        except Exception as exc:  # pragma: no cover - diagnostics
            bt.logging.warning({"miner_init": {"step": "dependency_check_failed", "error": str(exc)}})

        bt.logging.info({"miner_init": {"step": "summarizing_bittensor_state"}})
        try:
            summarize_miner_state(self)
        except Exception as exc:  # pragma: no cover - diagnostics
            bt.logging.warning({"miner_init": {"step": "summarize_state_failed", "error": str(exc)}})

        # Warn if allowing incoming requests from anyone.
        if not self.config.blacklist.force_validator_permit:
            bt.logging.warning({"miner_init": {"step": "blacklist_config", "allow_non_validators": True}})
            bt.logging.warning(
                "You are allowing non-validators to send requests to your miner. This is a security risk."
            )
        if self.config.blacklist.allow_non_registered:
            bt.logging.warning({"miner_init": {"step": "blacklist_config", "allow_non_registered": True}})
            bt.logging.warning(
                "You are allowing non-registered entities to send requests to your miner. This is a security risk."
            )

        # The axon handles request processing, allowing validators to send this miner requests.
        bt.logging.info(
            {
                "miner_init": {
                    "step": "creating_axon",
                    "axon_config": self._describe_axon_config(),
                }
            }
        )
        self.axon = bt.Axon(
            wallet=self.wallet,
            config=self.config() if callable(self.config) else self.config,
        )
        bt.logging.info({"miner_init": {"step": "axon_created", "axon": repr(self.axon)}})

        # Attach determiners which functions are called when servicing a request.
        # Use wrapper functions with explicit SparketSynapse type hints for bittensor's type resolution
        bt.logging.info({"miner_init": {"step": "attaching_handlers"}})
        
        async def _forward(synapse: SparketSynapse) -> SparketSynapse:
            return await self.forward(synapse)
        
        async def _blacklist(synapse: SparketSynapse) -> Tuple[bool, str]:
            return await self.blacklist(synapse)
        
        async def _priority(synapse: SparketSynapse) -> float:
            return await self.priority(synapse)
        
        self.axon.attach(
            forward_fn=_forward,
            blacklist_fn=_blacklist,
            priority_fn=_priority,
        )
        bt.logging.info({"miner_init": {"step": "handlers_attached"}})

        bt.logging.info(
            {
                "miner_init": {
                    "step": "components_initialized",
                    "elapsed_seconds": round(monotonic() - start_time, 3),
                }
            }
        )

        # Instantiate runners
        self.should_exit: bool = False
        self.is_running: bool = False
        self.thread: Union[threading.Thread, None] = None
        self.lock = asyncio.Lock()

    def run(self):
        """
        Initiates and manages the main loop for the miner on the Bittensor network. The main loop handles graceful shutdown on keyboard interrupts and logs unforeseen errors.

        This function performs the following primary tasks:
        1. Check for registration on the Bittensor network.
        2. Starts the miner's axon, making it active on the network.
        3. Periodically resynchronizes with the chain; updating the metagraph with the latest network state and setting weights.

        The miner continues its operations until `should_exit` is set to True or an external interruption occurs.
        During each epoch of its operation, the miner waits for new blocks on the Bittensor network, updates its
        knowledge of the network (metagraph), and sets its weights. This process ensures the miner remains active
        and up-to-date with the network's latest state.

        Note:
            - The function leverages the global configurations set during the initialization of the miner.
            - The miner's axon serves as its interface to the Bittensor network, handling incoming and outgoing requests.

        Raises:
            KeyboardInterrupt: If the miner is stopped by a manual interruption.
            Exception: For unforeseen errors during the miner's operation, which are logged for diagnosis.
        """

        # Check that miner is registered on the network.
        self._safe_sync(stage="startup_sync")

        served = self._safe_serve_axon()
        started = self._safe_start_axon()

        bt.logging.info(
            {
                "miner_runtime": {
                    "event": "startup_complete",
                    "axon_served": served,
                    "axon_started": started,
                    "block": self.block,
                }
            }
        )

        # This loop maintains the miner's operations until intentionally stopped.
        try:
            while not self.should_exit:
                while (
                    self.block - self.metagraph.last_update[self.uid]
                    < self.config.neuron.epoch_length
                ):
                    # Wait before checking again.
                    time.sleep(1)

                    # Check if we should exit.
                    if self.should_exit:
                        break

                # Sync metagraph and potentially set weights.
                self.sync()
                self.step += 1
                bt.logging.info(
                    {
                        "miner_runtime": {
                            "event": "sync_cooldown",
                            "seconds": METAGRAPH_SYNC_COOLDOWN_SECONDS,
                        }
                    }
                )
                time.sleep(METAGRAPH_SYNC_COOLDOWN_SECONDS)

        # If someone intentionally stops the miner, it'll safely terminate operations.
        except KeyboardInterrupt:
            self.axon.stop()
            bt.logging.success("Miner killed by keyboard interrupt.")
            exit()

        # In case of unforeseen errors, the miner will log the error and continue operations.
        except Exception as e:
            bt.logging.error(traceback.format_exc())

    def _safe_serve_axon(self) -> bool:
        try:
            bt.logging.info(
                {
                    "miner_runtime": {
                        "event": "serving_axon",
                        "chain_endpoint": getattr(self.config.subtensor, "chain_endpoint", None),
                        "netuid": getattr(self.config, "netuid", None),
                    }
                }
            )
            self.axon.serve(netuid=self.config.netuid, subtensor=self.subtensor)
            bt.logging.info({"miner_runtime": {"event": "axon_served"}})
            return True
        except Exception as exc:
            bt.logging.error({"miner_runtime": {"event": "axon_serve_error", "error": str(exc)}})
            return False

    def _safe_start_axon(self) -> bool:
        try:
            bt.logging.info({"miner_runtime": {"event": "starting_axon"}})
            self.axon.start()
            bt.logging.info({"miner_runtime": {"event": "axon_started"}})
            return True
        except Exception as exc:
            bt.logging.error({"miner_runtime": {"event": "axon_start_error", "error": str(exc)}})
            return False

    def _safe_sync(self, stage: str) -> None:
        try:
            bt.logging.info({"miner_runtime": {"event": "sync_start", "stage": stage}})
            sync_start = monotonic()
            self.sync()
            bt.logging.info(
                {
                    "miner_runtime": {
                        "event": "sync_complete",
                        "stage": stage,
                        "elapsed_seconds": round(monotonic() - sync_start, 3),
                        "block": self.block,
                    }
                }
            )
        except Exception as exc:
            bt.logging.error({"miner_runtime": {"event": "sync_error", "stage": stage, "error": str(exc)}})

    def _log_app_config(self) -> None:
        try:
            from sparket.config import sanitize_dict

            bt.logging.info(
                {
                    "miner_init": {
                        "step": "app_config_loaded",
                        "miner_settings": sanitize_dict(self.app_config.miner.model_dump()),
                    }
                }
            )
        except Exception:
            pass

    def _log_relevant_env(self) -> None:
        try:
            from sparket.config import sanitize_dict

            env_vars = {
                k: v
                for k, v in os.environ.items()
                if k.startswith(("SPARKET_", "BT_", "BITTENSOR_", "DATABASE_"))
            }
            bt.logging.info({"miner_env": sanitize_dict(env_vars)})
        except Exception:
            pass

    def _describe_axon_config(self) -> dict[str, object]:
        try:
            axon_cfg = getattr(self.config, "axon", None)
            return {
                "ip": getattr(axon_cfg, "ip", None),
                "port": getattr(axon_cfg, "port", None),
                "external_ip": getattr(axon_cfg, "external_ip", None),
                "external_port": getattr(axon_cfg, "external_port", None),
            }
        except Exception:
            return {}

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a separate background thread.
        This is useful for non-blocking operations.
        """
        if not self.is_running:
            bt.logging.debug("Starting miner in background thread.")
            self.should_exit = False
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            self.is_running = True
            bt.logging.debug("Started")

    def stop_run_thread(self):
        """
        Stops the miner's operations that are running in the background thread.
        """
        if self.is_running:
            bt.logging.debug("Stopping miner in background thread.")
            self.should_exit = True
            if self.thread is not None:
                self.thread.join(5)
            self.is_running = False
            bt.logging.debug("Stopped")

    def __enter__(self):
        """
        Starts the miner's operations in a background thread upon entering the context.
        This method facilitates the use of the miner in a 'with' statement.
        """
        self.run_in_background_thread()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Stops the miner's background operations upon exiting the context.
        This method facilitates the use of the miner in a 'with' statement.

        Args:
            exc_type: The type of the exception that caused the context to be exited.
                      None if the context was exited without an exception.
            exc_value: The instance of the exception that caused the context to be exited.
                       None if the context was exited without an exception.
            traceback: A traceback object encoding the stack trace.
                       None if the context was exited without an exception.
        """
        self.stop_run_thread()

    def resync_metagraph(self):
        """Resyncs the metagraph and updates the hotkeys and moving averages based on the new metagraph."""
        bt.logging.info("resync_metagraph()")

        # Sync the metagraph.
        self.metagraph.sync(subtensor=self.subtensor)

        # Keep ValidatorClient's metagraph reference fresh
        if hasattr(self, "validator_client"):
            self.validator_client._metagraph = self.metagraph
