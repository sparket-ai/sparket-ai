"""Tests for the SecurityMiddleware FastAPI integration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.requests import Request
from starlette.responses import Response
from starlette.testclient import TestClient
from fastapi import FastAPI

from sparket.validator.security.config import SecurityConfig
from sparket.validator.security.manager import SecurityManager
from sparket.validator.security.middleware import SecurityMiddleware, inject_security_middleware


class TestSecurityMiddleware:
    """Tests for the SecurityMiddleware class."""

    def _create_mock_request(
        self,
        path: str = "/SparketSynapse",
        hotkey: str = "test_hotkey_12345",
        ip: str = "192.168.1.100",
    ) -> MagicMock:
        """Create a mock request object."""
        request = MagicMock(spec=Request)
        request.url.path = path
        request.headers = {
            "bt_header_dendrite_hotkey": hotkey,
        }
        request.client = MagicMock()
        request.client.host = ip
        return request

    @pytest.mark.asyncio
    async def test_allowed_request_passes_through(self):
        """Test that allowed requests pass through to the next handler."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"test_hotkey_12345"}
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = self._create_mock_request()
        
        # Mock the call_next function
        expected_response = Response(content="OK", status_code=200)
        call_next = AsyncMock(return_value=expected_response)
        
        response = await middleware.dispatch(request, call_next)
        
        call_next.assert_called_once_with(request)
        assert response == expected_response

    @pytest.mark.asyncio
    async def test_blacklisted_hotkey_rejected(self):
        """Test that blacklisted hotkeys are rejected with 403."""
        manager = SecurityManager()
        manager._blacklist_hotkeys = {"test_hotkey_12345"}
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = self._create_mock_request()
        call_next = AsyncMock()
        
        response = await middleware.dispatch(request, call_next)
        
        # Should not call next handler
        call_next.assert_not_called()
        
        # Should return 403
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_unregistered_hotkey_rejected(self):
        """Test that unregistered hotkeys are rejected with 403."""
        manager = SecurityManager()
        manager._registered_hotkeys = {"other_hotkey"}
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = self._create_mock_request()
        call_next = AsyncMock()
        
        response = await middleware.dispatch(request, call_next)
        
        call_next.assert_not_called()
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_cooldown_rejected_with_429(self):
        """Test that hotkeys in cooldown are rejected with 429."""
        import time
        
        manager = SecurityManager()
        manager._registered_hotkeys = {"test_hotkey_12345"}
        
        # Put hotkey in cooldown
        record = manager._hotkey_records["test_hotkey_12345"]
        record.cooldown_until = time.time() + 60
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = self._create_mock_request()
        call_next = AsyncMock()
        
        response = await middleware.dispatch(request, call_next)
        
        call_next.assert_not_called()
        assert response.status_code == 429

    @pytest.mark.asyncio
    async def test_health_endpoint_bypasses_security(self):
        """Test that health check endpoints bypass security checks."""
        manager = SecurityManager()
        # No registered hotkeys, should normally reject
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        # Request to health endpoint
        request = self._create_mock_request(path="/health")
        
        expected_response = Response(content="OK", status_code=200)
        call_next = AsyncMock(return_value=expected_response)
        
        response = await middleware.dispatch(request, call_next)
        
        # Should pass through without security check
        call_next.assert_called_once()
        assert response == expected_response

    @pytest.mark.asyncio
    async def test_root_endpoint_bypasses_security(self):
        """Test that root endpoint bypasses security checks."""
        manager = SecurityManager()
        
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = self._create_mock_request(path="/")
        
        expected_response = Response(content="OK", status_code=200)
        call_next = AsyncMock(return_value=expected_response)
        
        response = await middleware.dispatch(request, call_next)
        
        call_next.assert_called_once()

    def test_get_client_ip_from_direct(self):
        """Test extracting IP from direct connection."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = MagicMock()
        request.headers = {}
        request.client = MagicMock()
        request.client.host = "192.168.1.100"
        
        ip = middleware._get_client_ip(request)
        assert ip == "192.168.1.100"

    def test_get_client_ip_from_x_forwarded_for(self):
        """Test extracting IP from X-Forwarded-For header."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = MagicMock()
        request.headers = {"x-forwarded-for": "10.0.0.1, 192.168.1.1"}
        request.client = MagicMock()
        request.client.host = "127.0.0.1"
        
        ip = middleware._get_client_ip(request)
        # Should get first IP in chain
        assert ip == "10.0.0.1"

    def test_get_client_ip_from_x_real_ip(self):
        """Test extracting IP from X-Real-IP header."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        request = MagicMock()
        request.headers = {"x-real-ip": "10.0.0.5"}
        request.client = MagicMock()
        request.client.host = "127.0.0.1"
        
        ip = middleware._get_client_ip(request)
        assert ip == "10.0.0.5"

    def test_is_synapse_endpoint(self):
        """Test synapse endpoint detection."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        # Should be synapse endpoints
        assert middleware._is_synapse_endpoint("/SparketSynapse") is True
        assert middleware._is_synapse_endpoint("/CustomSynapse") is True
        
        # Should NOT be synapse endpoints
        assert middleware._is_synapse_endpoint("/") is False
        assert middleware._is_synapse_endpoint("/health") is False
        assert middleware._is_synapse_endpoint("/healthz") is False
        assert middleware._is_synapse_endpoint("/ready") is False
        assert middleware._is_synapse_endpoint("/metrics") is False

    def test_rejection_message_blacklist(self):
        """Test rejection message for blacklisted entities."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        msg = middleware._get_rejection_message("hotkey_blacklisted")
        assert "blacklisted" in msg.lower()

    def test_rejection_message_not_registered(self):
        """Test rejection message for unregistered hotkeys."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        msg = middleware._get_rejection_message("not_registered")
        assert "not registered" in msg.lower()

    def test_rejection_message_cooldown(self):
        """Test rejection message for cooldown."""
        manager = SecurityManager()
        app = MagicMock()
        middleware = SecurityMiddleware(app, manager)
        
        msg = middleware._get_rejection_message("hotkey_cooldown:30")
        assert "cooldown" in msg.lower()
        assert "30" in msg


class TestInjectSecurityMiddleware:
    """Tests for the inject_security_middleware function."""

    def test_inject_middleware_adds_to_app(self):
        """Test that middleware is properly injected into axon."""
        manager = SecurityManager()
        
        # Create a mock axon with a FastAPI app
        mock_axon = MagicMock()
        mock_axon.app = FastAPI()
        
        # Inject middleware
        inject_security_middleware(mock_axon, manager)
        
        # Check that middleware was added
        # FastAPI/Starlette stores middleware in app.middleware_stack
        # We can verify by checking the app's middleware
        assert len(mock_axon.app.user_middleware) > 0

    def test_inject_middleware_no_app(self):
        """Test that missing app is handled gracefully."""
        manager = SecurityManager()
        
        mock_axon = MagicMock(spec=[])  # No app attribute
        
        # Should not raise
        inject_security_middleware(mock_axon, manager)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
