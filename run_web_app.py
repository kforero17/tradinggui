from src.config.ssl_config import configure_ssl_verification

# Configure SSL verification based on environment settings
# Set DISABLE_SSL_VERIFY=true only in corporate environments with trusted proxies
configure_ssl_verification()

from src.web_app.app import app
from src.config.settings import settings
from loguru import logger

def main():
    """Starts the Flask web application."""
    logger.info(f"Starting web app on http://{settings.WEB_APP_HOST}:{settings.WEB_APP_PORT}")
    
    app.run(
        host=settings.WEB_APP_HOST,
        port=settings.WEB_APP_PORT,
        debug=settings.WEB_APP_DEBUG
    )

if __name__ == "__main__":
    main() 