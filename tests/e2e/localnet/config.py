"""E2E localnet configuration.

Defines the network settings and registered wallets for E2E testing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class MinerConfig:
    """Configuration for a single miner."""
    
    wallet_name: str
    hotkey: str = "default"
    uid: int | None = None
    axon_port: int = 8094
    control_port: int = 8198
    
    @property
    def control_url(self) -> str:
        return f"http://127.0.0.1:{self.control_port}"


@dataclass
class ValidatorConfig:
    """Configuration for the validator."""
    
    wallet_name: str = "local-validator"
    hotkey: str = "default"
    uid: int = 1
    axon_port: int = 8093
    control_port: int = 8199
    
    @property
    def control_url(self) -> str:
        return f"http://127.0.0.1:{self.control_port}"


@dataclass
class LocalnetConfig:
    """Complete localnet configuration."""
    
    # Network settings
    chain_endpoint: str = "ws://127.0.0.1:9945"
    netuid: int = 2
    
    # Database settings
    database_host: str = "127.0.0.1"
    database_port: int = 5435
    database_name: str = "sparket_e2e"
    database_user: str = "sparket"
    database_password: str = "sparket"
    
    # Validator
    validator: ValidatorConfig = field(default_factory=lambda: ValidatorConfig(
        wallet_name="e2e-validator",
        uid=4,
    ))

    # Miners (registered on localnet)
    miners: List[MinerConfig] = field(default_factory=lambda: [
        MinerConfig(
            wallet_name="e2e-miner-2",
            uid=2,
            axon_port=8095,
            control_port=8197,
        ),
        MinerConfig(
            wallet_name="e2e-miner-3",
            uid=3,
            axon_port=8096,
            control_port=8196,
        ),
    ])
    
    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.database_user}:{self.database_password}"
            f"@{self.database_host}:{self.database_port}/{self.database_name}"
        )
    
    @property
    def validator_control_url(self) -> str:
        return self.validator.control_url
    
    def miner_control_url(self, index: int) -> str:
        return self.miners[index].control_url


# Default configuration
DEFAULT_CONFIG = LocalnetConfig()
