"""SSL verification configuration for TradingGui.

Configures SSL/TLS verification based on environment settings.
Used to support corporate proxy/VPN environments that use self-signed certificates.

IMPORTANT: This module must be imported BEFORE yfinance or any other library
that uses curl_cffi for HTTP requests.
"""
import os
import ssl
import urllib3
from loguru import logger
from .security import security_config


def _patch_curl_cffi_for_ssl_bypass() -> bool:
    """Monkey-patch curl_cffi Session to disable SSL verification.

    curl_cffi is a native libcurl binding that bypasses Python's SSL module.
    Setting environment variables is insufficient - we must patch the Session
    class to default verify=False for all instances.
    """
    try:
        from curl_cffi.requests.session import BaseSession

        original_init = BaseSession.__init__

        def patched_init(self, *args, **kwargs):
            if 'verify' not in kwargs:
                kwargs['verify'] = False
            original_init(self, *args, **kwargs)

        BaseSession.__init__ = patched_init
        logger.info("Patched curl_cffi Session to disable SSL verification")
        return True
    except ImportError:
        logger.warning("curl_cffi not installed, skipping patch")
        return False
    except Exception as e:
        logger.error(f"Error patching curl_cffi: {e}")
        return False


def configure_ssl_verification() -> None:
    """Configure SSL verification based on environment settings.

    This must be called BEFORE importing any libraries that make HTTPS requests
    (especially yfinance which uses curl_cffi).

    Security Warning:
        Disabling SSL verification allows man-in-the-middle attacks.
        Only disable in trusted corporate environments with validated proxies.
    """
    if security_config.should_disable_ssl():
        logger.warning("Disabling SSL verification - corporate proxy mode enabled")
        logger.warning("This configuration is INSECURE outside trusted networks")

        # 1. Patch curl_cffi FIRST (before any yfinance import)
        _patch_curl_cffi_for_ssl_bypass()

        # 2. Set environment variables for curl-based libraries
        os.environ['CURL_CA_BUNDLE'] = ''
        os.environ['REQUESTS_CA_BUNDLE'] = ''
        os.environ['SSL_CERT_FILE'] = ''

        # 3. Disable SSL verification for urllib3/requests
        ssl._create_default_https_context = ssl._create_unverified_context

        # 4. Suppress SSL warnings (they would flood logs)
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        logger.info("SSL verification disabled for: curl_cffi, requests, urllib3")
    else:
        logger.info("SSL verification enabled (secure mode)")
