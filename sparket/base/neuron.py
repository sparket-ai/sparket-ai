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

import copy
import ipaddress
import os
import typing
from urllib.parse import urlparse

import bittensor as bt

from abc import ABC, abstractmethod

# Sync calls set weights and also resyncs the metagraph.
from sparket.base.config import check_config, add_args, config
from sparket.shared.misc import ttl_get_block
from sparket import __spec_version__ as spec_version
from sparket.devtools.mock_bittensor import MockSubtensor, MockMetagraph


def _is_loopback_host(host: typing.Optional[str]) -> bool:
    if not host:
        return False
    value = host.strip().lower()
    if value in {"localhost"}:
        return True
    try:
        ip_obj = ipaddress.ip_address(value)
        return ip_obj.is_loopback
    except ValueError:
        return False


def _host_from_endpoint(endpoint: typing.Optional[str]) -> typing.Optional[str]:
    if not endpoint:
        return None
    try:
        parsed = urlparse(endpoint if "://" in endpoint else f"tcp://{endpoint}")
        return (parsed.hostname or "").strip() or None
    except Exception:
        return None


class BaseNeuron(ABC):
    """
    Base class for Bittensor miners. This class is abstract and should be inherited by a subclass. It contains the core logic for all neurons; validators and miners.

    In addition to creating a wallet, subtensor, and metagraph, this class also handles the synchronization of the network state via a basic checkpointing mechanism based on epoch length.
    """

    neuron_type: str = "BaseNeuron"

    @classmethod
    def check_config(cls, config: "bt.Config"):
        check_config(cls, config)

    @classmethod
    def add_args(cls, parser):
        add_args(cls, parser)

    @classmethod
    def config(cls):
        return config(cls)

    subtensor: "bt.Subtensor"
    wallet: "bt.Wallet"
    metagraph: "bt.Metagraph"
    spec_version: int = spec_version

    @property
    def block(self):
        return ttl_get_block(self)

    def __init__(self, config=None):
        # If a config is provided, use it directly; otherwise, build from defaults/CLI
        if config is not None:
            self.config = copy.deepcopy(config)
        else:
            self.config = self.config()
        try:
            bt.logging.info({
                "init_config_pre_check": {
                    "wallet": {
                        "name": getattr(self.config.wallet, "name", None),
                        "hotkey": getattr(self.config.wallet, "hotkey", None),
                    },
                    "netuid": getattr(self.config, "netuid", None),
                    "subtensor": {
                        "network": getattr(getattr(self.config, "subtensor", None), "network", None),
                        "chain_endpoint": getattr(getattr(self.config, "subtensor", None), "chain_endpoint", None),
                    },
                }
            })
        except Exception:
            pass
        # Apply YAML overrides for wallet/subtensor/netuid if still defaults
        settings = None
        try:
            from sparket.config.core import load_settings, sanitize_dict, last_yaml_path
            settings = load_settings()
            # Log loaded YAML (sanitized)
            try:
                yaml_snapshot = {
                    "path": last_yaml_path(),
                    "role": getattr(settings, "role", None),
                    "chain": settings.chain.model_dump() if getattr(settings, "chain", None) else None,
                    "subtensor": settings.subtensor.model_dump() if getattr(settings, "subtensor", None) else None,
                    "wallet": settings.wallet.model_dump() if getattr(settings, "wallet", None) else None,
                    "axon": settings.axon.model_dump() if getattr(settings, "axon", None) else None,
                }
                bt.logging.info({"yaml_source": sanitize_dict({k: v for k, v in yaml_snapshot.items() if v is not None})})
            except Exception:
                pass
            # Snapshot before override
            try:
                bt.logging.info({
                    "pre_yaml_config": {
                        "wallet": {
                            "name": getattr(self.config.wallet, "name", None),
                            "hotkey": getattr(self.config.wallet, "hotkey", None),
                        },
                        "netuid": getattr(self.config, "netuid", None),
                        "subtensor": {
                            "network": getattr(getattr(self.config, "subtensor", None), "network", None),
                            "chain_endpoint": getattr(getattr(self.config, "subtensor", None), "chain_endpoint", None),
                        },
                    }
                })
            except Exception:
                pass
            # wallet name/hotkey (YAML wins)
            if getattr(settings, "wallet", None):
                if settings.wallet.name:
                    self.config.wallet.name = settings.wallet.name
                if settings.wallet.hotkey:
                    self.config.wallet.hotkey = settings.wallet.hotkey
            # netuid (YAML wins if provided)
            if getattr(settings, "chain", None) and settings.chain.netuid is not None:
                self.config.netuid = settings.chain.netuid
            # subtensor network/endpoint (endpoint wins)
            if getattr(settings, "subtensor", None) or getattr(settings, "chain", None):
                endpoint = None
                if getattr(settings, "subtensor", None) and settings.subtensor.chain_endpoint:
                    endpoint = settings.subtensor.chain_endpoint
                elif getattr(settings, "chain", None) and settings.chain.endpoint:
                    endpoint = settings.chain.endpoint
                if endpoint:
                    self.config.subtensor.chain_endpoint = endpoint
                if getattr(settings, "subtensor", None) and settings.subtensor.network:
                    self.config.subtensor.network = settings.subtensor.network
            
            # Environment variables have HIGHEST priority (override everything)
            env_wallet_name = os.getenv("SPARKET_WALLET__NAME")
            env_wallet_hotkey = os.getenv("SPARKET_WALLET__HOTKEY")
            env_axon_port = os.getenv("SPARKET_AXON__PORT")
            env_axon_host = os.getenv("SPARKET_AXON__HOST")
            if env_wallet_name:
                self.config.wallet.name = env_wallet_name
            if env_wallet_hotkey:
                self.config.wallet.hotkey = env_wallet_hotkey
            if env_axon_port:
                self.config.axon.port = int(env_axon_port)
                self.config.axon.external_port = int(env_axon_port)
            if env_axon_host:
                self.config.axon.ip = env_axon_host
                self.config.axon.external_ip = env_axon_host
            
            # Snapshot after override
            try:
                bt.logging.info({
                    "effective_config": {
                        "wallet": {
                            "name": getattr(self.config.wallet, "name", None),
                            "hotkey": getattr(self.config.wallet, "hotkey", None),
                        },
                        "netuid": getattr(self.config, "netuid", None),
                        "subtensor": {
                            "network": getattr(getattr(self.config, "subtensor", None), "network", None),
                            "chain_endpoint": getattr(getattr(self.config, "subtensor", None), "chain_endpoint", None),
                        },
                        "axon": {
                            "ip": getattr(getattr(self.config, "axon", None), "ip", None),
                            "port": getattr(getattr(self.config, "axon", None), "port", None),
                            "external_ip": getattr(getattr(self.config, "axon", None), "external_ip", None),
                            "external_port": getattr(getattr(self.config, "axon", None), "external_port", None),
                        },
                    }
                })
            except Exception:
                pass
        except Exception:
            pass

        # Force axon bindings to loopback when operating on local network
        try:
            config_subtensor = getattr(self.config, "subtensor", None)
            config_axon = getattr(self.config, "axon", None)
            settings_ax = getattr(settings, "axon", None) if settings else None
            desired_endpoint = getattr(config_subtensor, "chain_endpoint", None) if config_subtensor else None
            derived_host = _host_from_endpoint(desired_endpoint)
            settings_net = getattr(getattr(settings, "subtensor", None), "network", None) if settings else None
            wants_local = False
            if settings_net and isinstance(settings_net, str) and settings_net.lower() == "local":
                wants_local = True
            if _is_loopback_host(derived_host):
                wants_local = True
            config_net = getattr(config_subtensor, "network", None) if config_subtensor else None
            if not wants_local and isinstance(config_net, str) and config_net.lower() == "local":
                wants_local = True
            if wants_local and config_axon is not None:
                def _to_int(value: typing.Any) -> typing.Optional[int]:
                    try:
                        if value is None:
                            return None
                        return int(value)
                    except (TypeError, ValueError):
                        return None
                override_host = getattr(settings_ax, "host", None) if settings_ax else None
                if not override_host:
                    default_host = "0.0.0.0" if neuron_type == "minerneuron" else "127.0.0.1"
                    override_host = derived_host or default_host
                if neuron_type != "minerneuron" and not _is_loopback_host(override_host):
                    override_host = "127.0.0.1"
                neuron_type = (getattr(self, "neuron_type", "") or "").lower()
                if neuron_type == "minerneuron":
                    default_port = 8094
                    legacy_ports = {None, 0, 8091, 8093}
                else:
                    default_port = 8093
                    legacy_ports = {None, 0, 8091}

                port_from_settings = False
                if settings_ax and getattr(settings_ax, "port", None) is not None:
                    configured_port = _to_int(settings_ax.port)
                    port_from_settings = True
                else:
                    existing_port = _to_int(getattr(config_axon, "port", None))
                    configured_port = default_port if existing_port in legacy_ports else existing_port

                    if configured_port in legacy_ports and getattr(config_axon, "external_port", None):
                        ext_port = _to_int(getattr(config_axon, "external_port", None))
                        if ext_port not in legacy_ports:
                            configured_port = ext_port

                    if configured_port in legacy_ports:
                        configured_port = default_port
                if (not port_from_settings and configured_port in legacy_ports) or configured_port is None:
                    configured_port = default_port
                port_value = int(configured_port)
                setattr(config_axon, "ip", override_host)
                setattr(config_axon, "external_ip", override_host)
                setattr(config_axon, "port", port_value)
                setattr(config_axon, "external_port", port_value)
                bt.logging.info({
                    "axon_override": {
                        "reason": "local_network",
                        "ip": override_host,
                        "port": port_value,
                        "endpoint_host": derived_host,
                        "network": settings_net,
                    }
                })
        except Exception:
            pass
        self.check_config(self.config)

        # Set up logging with the provided configuration.
        bt.logging.set_config(config=self.config.logging)

        # If a gpu is required, set the device to cuda:N (e.g. cuda:0)
        self.device = self.config.neuron.device

        # Log the configuration for reference.
        bt.logging.info(self.config)

        # Build Bittensor objects
        # These are core Bittensor classes to interact with the network.
        bt.logging.info("Setting up bittensor objects.")

        # The wallet holds the cryptographic key pairs for the miner.
        if self.config.mock:
            self.wallet = bt.Wallet(config=self.config)
            self.subtensor = MockSubtensor(
                self.config.netuid, wallet=self.wallet
            )
            self.metagraph = MockMetagraph(
                self.config.netuid, subtensor=self.subtensor
            )
        else:
            self.wallet = bt.Wallet(config=self.config)
            self.subtensor = bt.Subtensor(config=self.config)
            self.metagraph = self.subtensor.metagraph(self.config.netuid)

        bt.logging.info(f"Wallet: {self.wallet}")
        bt.logging.info(f"Subtensor: {self.subtensor}")
        bt.logging.info(f"Metagraph: {self.metagraph}")
        try:
            expected_endpoint = getattr(getattr(self.config, "subtensor", None), "chain_endpoint", None)
            expected_network = getattr(getattr(self.config, "subtensor", None), "network", None)
            actual_endpoint = getattr(self.subtensor, "chain_endpoint", None)
            actual_network = getattr(self.subtensor, "network", None)
            info = {
                "config": {
                    "network": expected_network,
                    "chain_endpoint": expected_endpoint,
                    "axon": {
                        "ip": getattr(getattr(self.config, "axon", None), "ip", None),
                        "port": getattr(getattr(self.config, "axon", None), "port", None),
                        "external_ip": getattr(getattr(self.config, "axon", None), "external_ip", None),
                        "external_port": getattr(getattr(self.config, "axon", None), "external_port", None),
                    },
                },
                "runtime": {
                    "network": actual_network,
                    "chain_endpoint": actual_endpoint,
                },
                "metagraph": {
                    "n": getattr(self.metagraph, "n", None),
                    "recent_block": getattr(self.metagraph, "block", None),
                },
            }
            bt.logging.info({"bittensor_connectivity": info})
            def _is_local(endpoint: typing.Optional[str]) -> bool:
                if not endpoint:
                    return False
                endpoint_lower = endpoint.lower()
                return "127.0.0.1" in endpoint_lower or "localhost" in endpoint_lower
            if expected_endpoint and _is_local(expected_endpoint) and actual_endpoint and not _is_local(actual_endpoint):
                bt.logging.warning({
                    "endpoint_mismatch": {
                        "expected_local": expected_endpoint,
                        "runtime": actual_endpoint,
                    }
                })
            if expected_network and expected_network.lower() == "local" and actual_network and actual_network.lower() not in {"local", "mock"}:
                bt.logging.warning({
                    "network_mismatch": {
                        "expected": expected_network,
                        "runtime": actual_network,
                    }
                })
        except Exception:
            pass

        # Check if the miner is registered on the Bittensor network before proceeding further.
        self.check_registered()

        # Each miner gets a unique identity (UID) in the network for differentiation.
        self.uid = self.metagraph.hotkeys.index(
            self.wallet.hotkey.ss58_address
        )
        bt.logging.info(
            f"Running neuron on subnet: {self.config.netuid} with uid {self.uid} using network: {self.subtensor.chain_endpoint}"
        )
        self.step = 0

    @abstractmethod
    async def forward(self, synapse: bt.Synapse) -> bt.Synapse:
        ...

    @abstractmethod
    def run(self):
        ...

    def sync(self):
        """
        Wrapper for synchronizing the state of the network for the given miner or validator.
        """
        # Ensure miner or validator hotkey is still registered on the network.
        self.check_registered()

        if self.should_sync_metagraph():
            if(self.resync_metagraph()):
                pass

        if self.should_set_weights():
            self.set_weights()

        # Always save state.
        self.save_state()

    def check_registered(self):
        # --- Check for registration.
        if not self.subtensor.is_hotkey_registered(
            netuid=self.config.netuid,
            hotkey_ss58=self.wallet.hotkey.ss58_address,
        ):
            bt.logging.error(
                f"Wallet: {self.wallet} is not registered on netuid {self.config.netuid}."
                f" Please register the hotkey using `btcli subnets register` before trying again"
            )
            exit()

    def should_sync_metagraph(self):
        """
        Check if enough epoch blocks have elapsed since the last checkpoint to sync.
        """
        return (
            self.block - self.metagraph.last_update[self.uid]
        ) > self.config.neuron.epoch_length

    def should_set_weights(self) -> bool:
        # Don't set weights on initialization.
        if self.step == 0:
            return False

        # Check if enough epoch blocks have elapsed since the last epoch.
        if self.config.neuron.disable_set_weights:
            return False

        # Define appropriate logic for when set weights.
        return (
            (self.block - self.metagraph.last_update[self.uid])
            > self.config.neuron.epoch_length
            and self.neuron_type != "MinerNeuron"
        )  # don't set weights if you're a miner

    def resync_metagraph(self) -> bool:
        """Default metagraph resync; validators/miners may override.

        Returns True on successful sync, False otherwise.
        """
        try:
            self.metagraph.sync(subtensor=self.subtensor)
            return True
        except Exception as e:
            bt.logging.warning({"metagraph_resync_error": str(e)})
            return False

    def save_state(self):
        bt.logging.trace(
            "save_state() not implemented for this neuron. You can implement this function to save model checkpoints or other useful data."
        )

    def load_state(self):
        bt.logging.trace(
            "load_state() not implemented for this neuron. You can implement this function to load model checkpoints or other useful data."
        )
