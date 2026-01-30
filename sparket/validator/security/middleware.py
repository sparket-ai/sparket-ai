"""FastAPI middleware for early request rejection.

Injects before bittensor's AxonMiddleware to reject bad actors
at the HTTP level, before full synapse deserialization.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

import bittensor as bt

from sparket.shared.log_colors import LogColors

if TYPE_CHECKING:
    from .manager import SecurityManager


class SecurityMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware for early security checks.
    
    Runs BEFORE bittensor's AxonMiddleware to reject requests
    based on headers alone, without parsing the request body.
    
    Checks performed:
    1. Permanent blacklist (hotkey and IP)
    2. Metagraph registration
    3. Cooldown status
    
    All checks are in-memory for performance.
    """
    
    def __init__(self, app: Any, security_manager: "SecurityManager"):
        super().__init__(app)
        self.security_manager = security_manager
    
    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Process request through security checks before routing."""
        
        # Extract identifiers from request
        # Bittensor uses these header names for dendrite info
        hotkey = request.headers.get("bt_header_dendrite_hotkey")
        
        # Get client IP (handle proxies)
        ip = self._get_client_ip(request)
        
        # Skip security checks for non-synapse endpoints (health checks, etc.)
        path = request.url.path
        if not self._is_synapse_endpoint(path):
            return await call_next(request)
        
        # Perform security check
        allowed, reason = self.security_manager.check_request(hotkey, ip)
        
        if not allowed:
            # Log the rejection
            hotkey_short = hotkey[:16] + "..." if hotkey and len(hotkey) > 16 else hotkey
            bt.logging.debug(
                f"{LogColors.MINER_LABEL} security_rejected: "
                f"hotkey={hotkey_short}, ip={ip}, reason={reason}"
            )
            
            # Return early rejection response
            # Using 403 Forbidden for blacklist/registration
            # Using 429 Too Many Requests for cooldowns
            status_code = 429 if reason and "cooldown" in reason else 403
            
            return JSONResponse(
                status_code=status_code,
                content={
                    "success": False,
                    "error": reason or "rejected",
                    "message": self._get_rejection_message(reason),
                },
            )
        
        # Request passed security checks, continue to next middleware
        return await call_next(request)
    
    def _get_client_ip(self, request: Request) -> Optional[str]:
        """Extract client IP, handling reverse proxies."""
        # Check X-Forwarded-For header (set by proxies)
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            # Take the first IP in the chain (original client)
            return forwarded.split(",")[0].strip()
        
        # Check X-Real-IP header (nginx)
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip.strip()
        
        # Fall back to direct connection
        if request.client:
            return request.client.host
        
        return None
    
    def _is_synapse_endpoint(self, path: str) -> bool:
        """Check if the path is a synapse endpoint that needs security checks.
        
        Skip security for health checks, metrics, etc.
        """
        # Bittensor synapse endpoints are typically /{SynapseName}
        # Skip root, health, and other utility endpoints
        skip_paths = {"/", "/health", "/healthz", "/ready", "/metrics", "/favicon.ico"}
        
        if path in skip_paths:
            return False
        
        # Skip if path is empty or just whitespace
        if not path or path.strip() == "/":
            return False
        
        return True
    
    def _get_rejection_message(self, reason: Optional[str]) -> str:
        """Get human-readable rejection message."""
        if not reason:
            return "Request rejected"
        
        if "blacklist" in reason:
            return "Your hotkey or IP has been blacklisted due to repeated violations"
        
        if "not_registered" in reason:
            return "Your hotkey is not registered on the subnet metagraph"
        
        if "cooldown" in reason:
            # Extract remaining time if present
            if ":" in reason:
                _, remaining = reason.split(":", 1)
                return f"You are in cooldown. Try again in {remaining} seconds"
            return "You are in cooldown due to repeated failures. Try again later"
        
        return f"Request rejected: {reason}"


def inject_security_middleware(
    axon: Any,
    security_manager: "SecurityManager",
) -> None:
    """Inject security middleware into an existing bittensor Axon.
    
    Must be called AFTER bt.Axon() creation but BEFORE axon.start().
    
    Usage:
        axon = bt.Axon(wallet=wallet, config=config)
        inject_security_middleware(axon, security_manager)
        axon.attach(forward_fn=my_forward)
        axon.start()
    """
    if not hasattr(axon, "app"):
        bt.logging.warning({
            "security_middleware": "axon_has_no_app",
            "skipping": True,
        })
        return
    
    # FastAPI middleware is processed in LIFO order (last added = first executed)
    # So adding our middleware after bittensor's means ours runs first
    axon.app.add_middleware(SecurityMiddleware, security_manager=security_manager)
    
    bt.logging.info({
        "security_middleware": "injected",
        "enforce_registration": security_manager.config.enforce_registration,
        "enforce_cooldowns": security_manager.config.enforce_cooldowns,
        "enforce_blacklist": security_manager.config.enforce_blacklist,
    })


__all__ = ["SecurityMiddleware", "inject_security_middleware"]
