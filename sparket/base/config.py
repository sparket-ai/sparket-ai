# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2023 Opentensor Foundation

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
import subprocess
import argparse
import bittensor as bt
from sparket.shared.logging import setup_events_logger


def is_cuda_available():
    try:
        output = subprocess.check_output(
            ["nvidia-smi", "-L"], stderr=subprocess.STDOUT
        )
        if "NVIDIA" in output.decode("utf-8"):
            return "cuda"
    except Exception:
        pass
    try:
        output = subprocess.check_output(["nvcc", "--version"]).decode("utf-8")
        if "release" in output:
            return "cuda"
    except Exception:
        pass
    return "cpu"


def check_config(cls, config: "bt.Config"):
    r"""Checks/validates the config namespace object."""
    

    # Ensure YAML-driven wallet/subtensor/netuid are applied before path resolution
    try:
        import os
        from sparket.config.core import load_settings
        settings = load_settings()
        if getattr(settings, "wallet", None):
            if settings.wallet.name:
                config.wallet.name = settings.wallet.name
            if getattr(settings.wallet, "hotkey", None):
                config.wallet.hotkey = settings.wallet.hotkey
        if getattr(settings, "chain", None) and settings.chain.netuid is not None:
            config.netuid = settings.chain.netuid
        endpoint = None
        if getattr(settings, "subtensor", None) and settings.subtensor.chain_endpoint:
            endpoint = settings.subtensor.chain_endpoint
        elif getattr(settings, "chain", None) and settings.chain.endpoint:
            endpoint = settings.chain.endpoint
        if endpoint:
            config.subtensor.chain_endpoint = endpoint
        if getattr(settings, "subtensor", None) and settings.subtensor.network:
            config.subtensor.network = settings.subtensor.network
        if getattr(settings, "axon", None) and getattr(config, "axon", None):
            if settings.axon.host:
                config.axon.ip = settings.axon.host
                config.axon.external_ip = settings.axon.host
            if settings.axon.port is not None:
                config.axon.port = int(settings.axon.port)
                config.axon.external_port = int(settings.axon.port)
        
        # Environment variables have HIGHEST priority (override YAML)
        env_wallet_name = os.getenv("SPARKET_WALLET__NAME")
        env_wallet_hotkey = os.getenv("SPARKET_WALLET__HOTKEY")
        env_axon_port = os.getenv("SPARKET_AXON__PORT")
        env_axon_host = os.getenv("SPARKET_AXON__HOST")
        if env_wallet_name:
            config.wallet.name = env_wallet_name
        if env_wallet_hotkey:
            config.wallet.hotkey = env_wallet_hotkey
        if env_axon_port and getattr(config, "axon", None):
            config.axon.port = int(env_axon_port)
            config.axon.external_port = int(env_axon_port)
        if env_axon_host and getattr(config, "axon", None):
            config.axon.ip = env_axon_host
            config.axon.external_ip = env_axon_host
    except Exception:
        pass

    full_path = os.path.expanduser(
        "{}/{}/{}/netuid{}/{}".format(
            config.logging.logging_dir,
            config.wallet.name,
            config.wallet.hotkey,
            config.netuid,
            config.neuron.name,
        )
    )
    bt.logging.info("full path:", full_path)
    config.neuron.full_path = os.path.expanduser(full_path)
    if not os.path.exists(config.neuron.full_path):
        os.makedirs(config.neuron.full_path, exist_ok=True)

    if not config.neuron.dont_save_events:
        # Add custom event logger for the events.
        events_logger = setup_events_logger(
            config.neuron.full_path, config.neuron.events_retention_size
        )
        bt.logging.register_primary_logger(events_logger.name)


def add_args(cls, parser):
    """
    Adds relevant arguments to the parser for operation.
    """

    parser.add_argument("--netuid", type=int, help="Subnet netuid", default=1)

    parser.add_argument(
        "--neuron.device",
        type=str,
        help="Device to run on.",
        default=is_cuda_available(),
    )

    parser.add_argument(
        "--neuron.epoch_length",
        type=int,
        help="The default epoch length (how often we set weights, measured in 12 second blocks).",
        default=100,
    )

    parser.add_argument(
        "--mock",
        action="store_true",
        help="Mock neuron and all network components.",
        default=False,
    )

    parser.add_argument(
        "--neuron.events_retention_size",
        type=str,
        help="Events retention size.",
        default=2 * 1024 * 1024 * 1024,  # 2 GB
    )

    parser.add_argument(
        "--neuron.dont_save_events",
        action="store_true",
        help="If set, we dont save events to a log file.",
        default=False,
    )

    parser.add_argument(
        "--wandb.off",
        action="store_true",
        help="Turn off wandb.",
        default=False,
    )

    parser.add_argument(
        "--wandb.offline",
        action="store_true",
        help="Runs wandb in offline mode.",
        default=False,
    )

    parser.add_argument(
        "--wandb.notes",
        type=str,
        help="Notes to add to the wandb run.",
        default="",
    )


def add_miner_args(cls, parser):
    """Add miner specific arguments to the parser."""

    parser.add_argument(
        "--neuron.name",
        type=str,
        help="Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name. ",
        default="miner",
    )

    parser.add_argument(
        "--blacklist.force_validator_permit",
        dest="blacklist.force_validator_permit",
        action="store_true",
        help="Require incoming requests to include a validator permit.",
    )
    parser.add_argument(
        "--blacklist.allow_non_validators",
        dest="blacklist.force_validator_permit",
        action="store_false",
        help="Allow requests from non-validator hotkeys. (Dangerous!)",
    )
    parser.set_defaults(**{"blacklist.force_validator_permit": True})

    parser.add_argument(
        "--blacklist.allow_non_registered",
        action="store_true",
        help="If set, miners will accept queries from non registered entities. (Dangerous!)",
        default=False,
    )

    parser.add_argument(
        "--wandb.project_name",
        type=str,
        default="template-miners",
        help="Wandb project to log to.",
    )

    parser.add_argument(
        "--wandb.entity",
        type=str,
        default="opentensor-dev",
        help="Wandb entity to log to.",
    )


def add_validator_args(cls, parser):
    """Add validator specific arguments to the parser."""

    parser.add_argument(
        "--neuron.name",
        type=str,
        help="Trials for this neuron go in neuron.root / (wallet_cold - wallet_hot) / neuron.name. ",
        default="validator",
    )

    parser.add_argument(
        "--neuron.timeout",
        type=float,
        help="The timeout for each forward call in seconds.",
        default=10,
    )

    parser.add_argument(
        "--neuron.num_concurrent_forwards",
        type=int,
        help="The number of concurrent forwards running at any time.",
        default=1,
    )

    parser.add_argument(
        "--neuron.sample_size",
        type=int,
        help="The number of miners to query in a single step.",
        default=50,
    )

    parser.add_argument(
        "--neuron.disable_set_weights",
        action="store_true",
        help="Disables setting weights.",
        default=False,
    )

    parser.add_argument(
        "--neuron.moving_average_alpha",
        type=float,
        help="Moving average alpha parameter, how much to add of the new observation.",
        default=0.1,
    )

    parser.add_argument(
        "--neuron.axon_off",
        "--axon_off",
        action="store_true",
        # Note: the validator needs to serve an Axon with their IP or they may
        #   be blacklisted by the firewall of serving peers on the network.
        help="Set this flag to not attempt to serve an Axon.",
        default=False,
    )

    parser.add_argument(
        "--neuron.vpermit_tao_limit",
        type=int,
        help="The maximum number of TAO allowed to query a validator with a vpermit.",
        default=4096,
    )

    parser.add_argument(
        "--wandb.project_name",
        type=str,
        help="The name of the project where you are sending the new run.",
        default="template-validators",
    )

    parser.add_argument(
        "--wandb.entity",
        type=str,
        help="The name of the project where you are sending the new run.",
        default="opentensor-dev",
    )


def config(cls):
    """
    Returns the configuration object specific to this miner or validator after adding relevant arguments.
    """
    parser = argparse.ArgumentParser()
    bt.Wallet.add_args(parser)
    bt.Subtensor.add_args(parser)
    bt.logging.add_args(parser)
    bt.Axon.add_args(parser)
    cls.add_args(parser)
    parser.set_defaults(
        **{
            "axon.ip": "0.0.0.0",
            "axon.external_ip": "0.0.0.0",
        }
    )
    # Apply YAML-driven defaults for non-secret settings
    try:
        from sparket.config.core import load_settings
        settings = load_settings()
        # wallet.name
        if getattr(settings, "wallet", None) and settings.wallet.name:
            parser.set_defaults(**{"wallet.name": settings.wallet.name})
        # netuid
        if getattr(settings, "chain", None) and settings.chain.netuid is not None:
            parser.set_defaults(**{"netuid": settings.chain.netuid})
        # wallet.hotkey
        if getattr(settings, "wallet", None) and settings.wallet.hotkey:
            parser.set_defaults(**{"wallet.hotkey": settings.wallet.hotkey})
        # subtensor.network
        if getattr(settings, "subtensor", None) and settings.subtensor.network:
            parser.set_defaults(**{"subtensor.network": settings.subtensor.network})
        # chain endpoint override maps to subtensor.chain_endpoint default
        endpoint = None
        if getattr(settings, "subtensor", None) and settings.subtensor.chain_endpoint:
            endpoint = settings.subtensor.chain_endpoint
        elif getattr(settings, "chain", None) and settings.chain.endpoint:
            endpoint = settings.chain.endpoint
        if endpoint:
            parser.set_defaults(**{"subtensor.chain_endpoint": endpoint})
        if getattr(settings, "axon", None):
            if settings.axon.host:
                parser.set_defaults(**{
                    "axon.ip": settings.axon.host,
                    "axon.external_ip": settings.axon.host,
                })
            if settings.axon.port is not None:
                port_value = int(settings.axon.port)
                parser.set_defaults(**{
                    "axon.port": port_value,
                    "axon.external_port": port_value,
                })
    except Exception:
        pass
    return bt.Config(parser)
