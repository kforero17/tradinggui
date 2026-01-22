"""Centralized security configuration for TradingGui.

Manages secret keys and security settings via environment variables.
"""
import os
import secrets
from typing import Optional
from loguru import logger


class SecurityConfig:
    """Security configuration manager."""

    @staticmethod
    def get_secret_key() -> str:
        """Get Flask secret key from environment or generate one.

        Returns:
            Secret key string for Flask session management.
        """
        secret = os.environ.get('FLASK_SECRET_KEY')
        if not secret:
            logger.warning(
                "FLASK_SECRET_KEY not set. Using generated key. "
                "Sessions will not persist across restarts. "
                "Set FLASK_SECRET_KEY environment variable for production."
            )
            secret = secrets.token_hex(32)
        return secret

    @staticmethod
    def should_disable_ssl() -> bool:
        """Check if SSL verification should be disabled.

        Returns:
            True if SSL verification should be disabled (insecure).
        """
        disable = os.environ.get('DISABLE_SSL_VERIFY', 'false').lower()
        enabled = disable in ('true', '1', 'yes')
        if enabled:
            logger.warning(
                "SSL verification is DISABLED. This is insecure and allows MITM attacks. "
                "Only use in corporate environments with trusted proxies. "
                "Set DISABLE_SSL_VERIFY=false to enable SSL verification."
            )
        return enabled

    @staticmethod
    def get_auth_username() -> Optional[str]:
        """Get authentication username from environment.

        Returns:
            Username string or None if not configured.
        """
        return os.environ.get('FLASK_AUTH_USERNAME')

    @staticmethod
    def get_auth_password_hash() -> Optional[str]:
        """Get authentication password hash from environment.

        Returns:
            Password hash string or None if not configured.
        """
        return os.environ.get('FLASK_AUTH_PASSWORD_HASH')


security_config = SecurityConfig()
