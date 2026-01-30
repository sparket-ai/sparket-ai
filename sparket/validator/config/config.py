#subnet specific config file for validator

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from sparket.config import (
    APISettings,
    ChainSettings,
    DatabaseSettings,
    Settings,
    TimerSettings,
    load_settings,
)
from . import hyperparameters as validator_hyperparameters


class ValidatorSettings(BaseModel):
    hyperparameters: validator_hyperparameters.Hyperparameters = Field(
        default_factory=validator_hyperparameters.Hyperparameters
    )
    connection_push_interval_seconds: int = Field(default=300)
    scoring_worker_enabled: bool = Field(default=True)
    scoring_worker_count: int = Field(default=1, ge=1)
    scoring_worker_fallback: bool = Field(default=True)
    # Token authentication for miner push operations
    require_push_token: bool = Field(
        default=True,
        description="Require authentication token for miner odds/outcome pushes"
    )
    token_rotation_steps: int = Field(
        default=10,
        ge=1,
        description="Number of validator steps before rotating the push token"
    )
    proxy_url: str | None = Field(
        default=None,
        description="External URL if validator is behind a reverse proxy"
    )


class Config(BaseSettings):
    # Nested central settings
    core: Settings = Field(default_factory=lambda: load_settings(role="validator"))
    validator: ValidatorSettings = Field(default_factory=ValidatorSettings)

    # Compatibility aliases for existing imports/usages
    @property
    def api_port_base(self) -> int:
        return self.core.api.port_base

    @property
    def api_port_pool_size(self) -> int:
        return self.core.api.port_pool_size

    @property
    def api_port_rotation_enabled(self) -> bool:
        return self.core.api.port_rotation_enabled

    @property
    def api_port_rotation_interval_seconds(self) -> int:
        return self.core.api.port_rotation_interval_seconds

    @property
    def api_trust_proxy(self) -> bool:
        return self.core.api.trust_proxy

    @property
    def api_ip_allowlist(self) -> list[str]:
        return self.core.api.ip_allowlist

    @property
    def api_ip_blocklist(self) -> list[str]:
        return self.core.api.ip_blocklist

    @property
    def api_os_blacklist_enabled(self) -> bool:
        return self.core.api.os_blacklist_enabled

    @property
    def api_os_blacklist_tool(self) -> str:
        return self.core.api.os_blacklist_tool

    # Database convenience
    @property
    def database_url(self) -> str | None:
        return self.core.database.url or None

    @property
    def database_name(self) -> str | None:
        return self.core.database.name or None