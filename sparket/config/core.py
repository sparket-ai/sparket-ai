from __future__ import annotations

import os
import re
from typing import Any, Callable, Dict, Iterable, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator, AliasChoices, ConfigDict
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)
from urllib.parse import urlsplit, urlunsplit


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _project_root() -> str:
    """Return the project root directory (where pyproject.toml lives)."""
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _data_dir(test_mode: bool = False) -> str:
    """Return the data directory based on test mode."""
    mode = "test" if test_mode else "prod"
    return os.path.join(_project_root(), "sparket", "data", mode)


class APISettings(BaseModel):
    # Optional proxy for validator announcements (e.g., Cloudflare tunnel)
    # If set, validator will advertise this URL instead of raw host/port.
    proxy_url: str | None = None
    # Require signed token in miner pushes to validator; token is rotated periodically
    require_push_token: bool = True


class DatabaseSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    # Allow both SPARKET_DATABASE__URL and bare DATABASE_URL
    url: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__URL", "DATABASE_URL"),
    )
    # Logical database name (used both for composition and bootstrap)
    name: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__NAME", "SPARKET_DATABASE_NAME"),
    )

    # Optional discrete connection fields for composing URL when url is not provided
    host: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__HOST", "SPARKET_DATABASE_HOST"),
    )
    port: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__PORT", "SPARKET_DATABASE_PORT"),
    )
    user: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__USER", "SPARKET_DATABASE_USER"),
    )
    password: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_DATABASE__PASSWORD", "SPARKET_DATABASE_PASSWORD"),
    )

    pool_size: int = 50
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 1800
    echo: bool = False

    class DatabaseBootstrap(BaseModel):
        auto_create: bool = True
        admin_url: Optional[str] = Field(default=None)

    bootstrap: DatabaseBootstrap = Field(default_factory=DatabaseBootstrap)

    class DatabaseDocker(BaseModel):
        enabled: bool = True
        image: str = "postgres:18"
        port: int = 5435  # Host port for postgres container
        container_name: str = "pg-sparket"
        volume: str = "pg_sparket_data"

        # Test mode uses same container, just different database name
        def effective_port(self, test_mode: bool = False) -> int:
            return self.port  # Same port for both

        def effective_container_name(self, test_mode: bool = False) -> str:
            return self.container_name  # Same container for both

        def effective_volume(self, test_mode: bool = False) -> str:
            return self.volume  # Same volume for both

    docker: DatabaseDocker = Field(default_factory=DatabaseDocker)

    # Test mode uses separate database name within same container
    test_name: Optional[str] = Field(default="sparket_test")

    @field_validator("url")
    @classmethod
    def _validate_url(cls, v: Optional[str]) -> Optional[str]:
        if v is None or v == "":
            return None
        # Require async driver for SQLAlchemy async engine
        if not v.startswith("postgresql+asyncpg://"):
            raise ValueError(
                "database.url must be an async Postgres URL, e.g. postgresql+asyncpg://user:pass@host:5432/db"
            )
        return v

    @model_validator(mode="after")
    def _compose_url_if_missing(self) -> "DatabaseSettings":
        if (self.url is None or self.url == "") and self.user and self.name:
            host = self.host or "localhost"
            port = self.port or 5432
            pwd = self.password or ""
            # Construct URL; if password empty, omit colon to avoid trailing colon
            auth = f"{self.user}:{pwd}" if pwd else f"{self.user}"
            self.url = f"postgresql+asyncpg://{auth}@{host}:{int(port)}/{self.name}"
        return self


class ChainSettings(BaseModel):
    # Kept minimal to avoid clashing with bittensor CLI config
    netuid: int = 1
    # Optional chain endpoint override (normally comes from bittensor config)
    endpoint: Optional[str] = None


class WalletSettings(BaseModel):
    name: Optional[str] = None
    hotkey: Optional[str] = None
    # Optional: path and hotkey can be extended later


class SubtensorSettings(BaseModel):
    network: Optional[str] = None  # finney | test | local
    chain_endpoint: Optional[str] = None


class AxonSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    host: Optional[str] = Field(
        default="0.0.0.0",
        validation_alias=AliasChoices("SPARKET_AXON__HOST", "SPARKET_AXON_HOST"),
    )
    port: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_AXON__PORT", "SPARKET_AXON_PORT"),
    )
    external_ip: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_AXON__EXTERNAL_IP", "SPARKET_AXON_EXTERNAL_IP"),
    )
    external_port: Optional[int] = Field(
        default=None,
        validation_alias=AliasChoices("SPARKET_AXON__EXTERNAL_PORT", "SPARKET_AXON_EXTERNAL_PORT"),
    )


class RuntimeSettings(BaseModel):
    """Runtime mode settings for test vs production environments."""
    model_config = ConfigDict(populate_by_name=True)
    test_mode: bool = Field(default=False)

    @model_validator(mode="before")
    @classmethod
    def _load_from_env(cls, data: Any) -> Any:
        """Load test_mode from environment variables."""
        if isinstance(data, dict):
            data = dict(data)
        else:
            data = {}
        # Check environment variables for test_mode
        env_val = _env("SPARKET_TEST_MODE") or _env("TEST_MODE")
        if env_val is not None:
            # Parse boolean from string
            data["test_mode"] = env_val.lower() in ("true", "1", "yes", "on")
        return data

    @property
    def data_dir(self) -> str:
        """Return the appropriate data directory based on test mode."""
        return _data_dir(self.test_mode)


class TimerSettings(BaseModel):
    # MVP timing by steps; validator loop targets ~12s per step
    step_target_seconds: int = 12
    metagraph_sync_steps: int = 5
    main_score_steps: int = 25
    # Provider polling (legacy step-based, kept for closing snapshot cadence)
    provider_fetch_steps: int = 50
    provider_hot_window_minutes: int = 60
    provider_hot_fetch_steps: int = 10
    # SDIO background ingest interval (runs independently of main loop)
    sdio_ingest_interval_seconds: int = 60
    cleanup_steps: int = 200
    outcome_process_steps: int = 10

    @field_validator(
        "step_target_seconds",
        "metagraph_sync_steps",
        "main_score_steps",
        "provider_fetch_steps",
        "provider_hot_window_minutes",
        "provider_hot_fetch_steps",
        "sdio_ingest_interval_seconds",
        "cleanup_steps",
        "outcome_process_steps",
    )
    @classmethod
    def _validate_positive(cls, v: int, info: ValidationInfo) -> int:
        if v <= 0:
            raise ValueError(f"timers.{info.field_name} must be > 0 seconds")
        return v


_LAST_YAML_PATH: Optional[str] = None


def last_yaml_path() -> Optional[str]:
    return _LAST_YAML_PATH


class _YamlSettingsSource(PydanticBaseSettingsSource):
    def __init__(self, settings_cls: type[BaseSettings], role: Optional[str]) -> None:
        super().__init__(settings_cls)
        self.role = role
        self._data: Dict[str, Any] = self._load_yaml_dict()

    def _load_yaml_dict(self) -> Dict[str, Any]:
        role_key = f"SPARKET_{self.role.upper()}_CONFIG_FILE" if self.role else None
        candidates: list[Optional[str]] = []
        if role_key:
            val = _env(role_key)
            if val:
                candidates.append(val)
        generic = _env("SPARKET_CONFIG_FILE") or _env("SPARKET_CONFIG")
        if generic:
            candidates.append(generic)
        candidates.extend(
            [
                "./sparket.yaml",
                "./config/sparket.yaml",
                "./sparket/config/sparket.yaml",
                f"./{self.role}.yaml" if self.role else None,
                f"./config/{self.role}.yaml" if self.role else None,
                (f"./sparket/config/{self.role}.yaml" if self.role else None),
            ]
        )
        try:
            pkg_dir = os.path.dirname(__file__)
            candidates.extend(
                [
                    os.path.join(pkg_dir, "sparket.yaml"),
                    (os.path.join(pkg_dir, f"{self.role}.yaml") if self.role else None),
                ]
            )
        except Exception:
            pass
        global _LAST_YAML_PATH
        for path in filter(None, candidates):
            try:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8") as f:
                        data = yaml.safe_load(f) or {}
                        if isinstance(data, dict):
                            try:
                                import bittensor as bt  # lazy import for logging only
                                bt.logging.info({
                                    "yaml_loaded": {
                                        "path": path,
                                        "role": self.role or "",
                                    }
                                })
                            except Exception:
                                pass
                            _LAST_YAML_PATH = path
                            return data
            except Exception:
                _LAST_YAML_PATH = path
                return {}
        _LAST_YAML_PATH = None
        return {}

    def get_field_value(self, field, field_name: str):
        # Provide per-field values from the loaded YAML dict (top-level fields only)
        if not isinstance(self._data, dict):
            return None, None, None
        if field_name in self._data:
            return self._data[field_name], "yaml", False
        return None, None, None

    def __call__(self) -> Dict[str, Any]:
        # Fallback full-dict provider; used by older signatures
        return self._data


class Settings(BaseSettings):
    api: APISettings = Field(default_factory=APISettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    chain: ChainSettings = Field(default_factory=ChainSettings)
    wallet: WalletSettings = Field(default_factory=WalletSettings)
    subtensor: SubtensorSettings = Field(default_factory=SubtensorSettings)
    timers: TimerSettings = Field(default_factory=TimerSettings)
    axon: AxonSettings = Field(default_factory=AxonSettings)
    runtime: RuntimeSettings = Field(default_factory=RuntimeSettings)

    # Optional label for clarity/logging (validator/miner)
    role: Optional[Literal["validator", "miner", "base"]] = None

    @property
    def test_mode(self) -> bool:
        """Convenience property for test mode."""
        return self.runtime.test_mode

    @property
    def data_dir(self) -> str:
        """Convenience property for data directory."""
        return self.runtime.data_dir

    model_config = SettingsConfigDict(
        # Allow SPARKET_API__IP_ALLOWLIST, etc.; env wins over YAML
        env_prefix="SPARKET_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        *args,
        **kwargs,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Robustly adapt to pydantic-settings signatures across versions.

        Expected parameters (varies by version):
          - settings_cls, init_settings, env_settings, [dotenv_settings], file_secret_settings
        """
        # Extract params from kwargs if provided
        init_settings = kwargs.get("init_settings")
        env_settings = kwargs.get("env_settings")
        dotenv_settings = kwargs.get("dotenv_settings")
        file_secret_settings = kwargs.get("file_secret_settings")

        # Fallback to positional if kwargs absent
        if init_settings is None or env_settings is None:
            # args layout may be: (settings_cls, init, env, file_secret) or (settings_cls, init, env, dotenv, file_secret)
            if len(args) >= 3:
                init_settings = init_settings or args[1]
                env_settings = env_settings or args[2]
            if len(args) == 4:
                file_secret_settings = file_secret_settings or args[3]
            if len(args) >= 5:
                dotenv_settings = dotenv_settings or args[3]
                file_secret_settings = file_secret_settings or args[4]

        role = _env("SPARKET_ROLE")
        yaml_source = _YamlSettingsSource(cls, role)

        # Build ordered sources, skipping Nones
        sources: list[PydanticBaseSettingsSource] = [yaml_source]
        if env_settings is not None:
            sources.append(env_settings)
        if dotenv_settings is not None:
            sources.append(dotenv_settings)
        if init_settings is not None:
            sources.append(init_settings)
        if file_secret_settings is not None:
            sources.append(file_secret_settings)
        return tuple(sources)

    @model_validator(mode="after")
    def _validate_cross_fields(self) -> "Settings":
        # If database.url is not set, allow it to come from env DATABASE_URL at runtime
        # We already wired validation_alias to read DATABASE_URL
        # Resolve default chain endpoint from subtensor.network when not explicitly set
        if (self.subtensor and self.subtensor.chain_endpoint) and not self.chain.endpoint:
            self.chain.endpoint = self.subtensor.chain_endpoint
        if (self.subtensor and (not self.subtensor.chain_endpoint)) and (self.subtensor.network and not self.chain.endpoint):
            net = (self.subtensor.network or "").lower()
            if net == "finney":
                self.chain.endpoint = "wss://entrypoint-finney.opentensor.ai:443"
            elif net == "test":
                self.chain.endpoint = "wss://test.finney.opentensor.ai:443"
            elif net == "local":
                self.chain.endpoint = "http://localhost:9944/"
        return self


def load_settings(*, role: Optional[str] = None) -> Settings:
    # Allow explicit role; also set env var for YAML resolution within Settings
    if role and not _env("SPARKET_ROLE"):
        os.environ["SPARKET_ROLE"] = role
    return Settings()


_SECRET_KEY_PATTERN = re.compile(
    r"(password|passwd|secret|token|key|dsn|url|uri)", re.IGNORECASE
)


def _sanitize_url(value: str) -> str:
    try:
        parts = urlsplit(value)
        # Redact credentials; keep scheme/host/port/path for debugging
        netloc = parts.netloc
        if "@" in netloc:
            creds, host = netloc.split("@", 1)
            if ":" in creds:
                user, _ = creds.split(":", 1)
            else:
                user = creds
            redacted = f"{user}:***@{host}"
        else:
            redacted = netloc
        return urlunsplit((parts.scheme, redacted, parts.path, parts.query, parts.fragment))
    except Exception:
        return "***"


def sanitize_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    def _sanitize(obj: Any, parent_key: str | None = None) -> Any:
        if isinstance(obj, dict):
            return {k: _sanitize(v, k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_sanitize(v, parent_key) for v in obj]
        if isinstance(obj, str):
            # Heuristic: redact if key suggests secret or if looks like URL with creds
            if parent_key and _SECRET_KEY_PATTERN.search(parent_key):
                if parent_key.lower().endswith("url") or parent_key.lower().endswith("uri"):
                    return _sanitize_url(obj)
                return "***"
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", obj):
                return _sanitize_url(obj)
            return obj
        return obj

    return _sanitize(data)


