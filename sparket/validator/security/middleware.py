"""FastAPI middleware for early request rejection.

Injects before bittensor's AxonMiddleware to reject bad actors
at the HTTP level, before full synapse deserialization.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Optional

from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

import bittensor as bt

from sparket.shared.log_colors import LogColors
from .config import FailureType

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
        
        path = request.url.path
        
        # Check for scanner/probe requests (GET / or empty path without valid bittensor headers)
        # These are bots probing the endpoint - immediately blacklist the IP
        if self._is_scanner_request(request, path, hotkey, ip):
            return await self._handle_scanner_request(ip)
        
        # Skip security checks for legitimate non-synapse endpoints (health checks, etc.)
        if not self._is_synapse_endpoint(path):
            return await call_next(request)
        
        # Log all synapse requests for debugging (TRACE level to avoid spam)
        hotkey_short = hotkey[:16] + "..." if hotkey and len(hotkey) > 16 else hotkey
        bt.logging.trace({
            "security_check": {
                "hotkey": hotkey_short,
                "ip": ip,
                "path": path,
            }
        })
        
        # Perform security check
        allowed, reason = self.security_manager.check_request(hotkey, ip)
        
        if not allowed:
            # Log the rejection at WARNING level so it's visible
            hotkey_short = hotkey[:16] + "..." if hotkey and len(hotkey) > 16 else hotkey
            bt.logging.warning(
                f"{LogColors.MINER_LABEL} security_rejected: "
                f"hotkey={hotkey_short}, ip={ip}, reason={reason}"
            )
            
            # Return early rejection response
            # Using 403 Forbidden for blacklist/registration
            # Using 429 Too Many Requests for cooldowns
            is_cooldown = reason and "cooldown" in reason
            status_code = 429 if is_cooldown else 403
            
            # Track cooldown violations for fail2ban
            # If they keep submitting while in cooldown, trigger 24h ban
            if is_cooldown:
                asyncio.create_task(
                    self.security_manager.record_cooldown_violation(hotkey, ip)
                )
            
            # Build response content
            content: dict = {
                "success": False,
                "error": reason.split(":")[0] if reason else "rejected",
                "message": self._get_rejection_message(reason),
            }
            
            # Add structured cooldown info for miners to implement backoff
            headers: dict = {}
            if is_cooldown and reason and ":" in reason:
                try:
                    # reason format: "hotkey_cooldown:300s" or "ip_cooldown:300s"
                    cooldown_type, remaining_str = reason.split(":", 1)
                    remaining_sec = int(remaining_str.rstrip("s"))
                    content["retry_after"] = remaining_sec
                    content["cooldown_type"] = cooldown_type
                    content["cooldown_remaining_seconds"] = remaining_sec
                    # Standard HTTP header for rate limiting
                    headers["Retry-After"] = str(remaining_sec)
                except (ValueError, AttributeError):
                    pass
            
            return JSONResponse(
                status_code=status_code,
                content=content,
                headers=headers if headers else None,
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
    
    def _is_scanner_request(
        self,
        request: Request,
        path: str,
        hotkey: Optional[str],
        ip: Optional[str],
    ) -> bool:
        """Detect scanner/probe requests that should trigger immediate blacklist.
        
        Scanners typically:
        - Hit root path "/" with no valid bittensor headers
        - Make requests without proper synapse names
        - Probe common paths looking for vulnerabilities
        
        Legitimate bittensor clients always include dendrite headers.
        """
        # Paths that scanners commonly hit
        scanner_paths = {"/", "/admin", "/api", "/login", "/wp-admin", "/robots.txt"}
        
        # If hitting a scanner-common path without bittensor headers, it's a scanner
        if path in scanner_paths:
            # Check for any bittensor header presence
            has_bt_headers = any(
                h.startswith("bt_header_") for h in request.headers.keys()
            )
            
            # No bittensor headers + hitting root/common paths = scanner
            if not has_bt_headers:
                return True
            
            # Has bittensor headers but hitting "/" with no valid hotkey = malformed
            if path == "/" and not hotkey:
                return True
        
        return False
    
    async def _handle_scanner_request(self, ip: Optional[str]) -> Response:
        """Handle a detected scanner request by blacklisting and rejecting.
        
        Immediately blacklists the IP and returns a 403 response.
        """
        if ip:
            # Check if already blacklisted to avoid duplicate DB writes
            is_blocked, _ = self.security_manager.is_blacklisted(None, ip)
            
            if not is_blocked:
                bt.logging.warning(
                    f"{LogColors.MINER_LABEL} scanner_detected: "
                    f"ip={ip}, action=permanent_blacklist"
                )
                
                # Fire-and-forget blacklist (don't block the response)
                asyncio.create_task(
                    self.security_manager.add_to_blacklist(
                        identifier=ip,
                        identifier_type="ip",
                        reason="Scanner/probe request detected (GET / with no valid headers)",
                        failure_type=FailureType.SCANNER_REQUEST.value,
                        failure_count=1,
                    )
                )
        
        return JSONResponse(
            status_code=403,
            content={
                "success": False,
                "error": "forbidden",
                "message": "Access denied",
            },
        )
    
    def _is_synapse_endpoint(self, path: str) -> bool:
        """Check if the path is a synapse endpoint that needs security checks.
        
        Skip security for health checks, metrics, etc.
        Note: Scanner paths are handled separately in _is_scanner_request.
        """
        # Bittensor synapse endpoints are typically /{SynapseName}
        # Skip health and other utility endpoints
        skip_paths = {"/health", "/healthz", "/ready", "/metrics", "/favicon.ico"}
        
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
