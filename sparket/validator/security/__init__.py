"""Security module for validator rate limiting and blacklisting.

Provides:
- SecurityManager: Central coordinator for security checks
- SecurityMiddleware: FastAPI middleware for early rejection
- Configuration classes for customization
"""

from .config import (
    CooldownConfig,
    FailureType,
    RateLimitConfig,
    SecurityConfig,
    COOLDOWN_FAILURES,
    CRITICAL_FAILURES,
)
from .manager import SecurityManager
from .middleware import SecurityMiddleware, inject_security_middleware

__all__ = [
    # Manager
    "SecurityManager",
    # Middleware
    "SecurityMiddleware",
    "inject_security_middleware",
    # Config
    "CooldownConfig",
    "FailureType",
    "RateLimitConfig",
    "SecurityConfig",
    "COOLDOWN_FAILURES",
    "CRITICAL_FAILURES",
]
