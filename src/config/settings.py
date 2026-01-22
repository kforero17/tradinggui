from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Settings(BaseSettings):
    # Paths
    BASE_DIR: Path = Path(__file__).parent.parent.parent
    DATA_DIR: Path = BASE_DIR / "data"
    RAW_DATA_DIR: Path = DATA_DIR / "raw"
    PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
    
    # Database
    DB_PATH: Path = PROCESSED_DATA_DIR / "stock_metrics.db"
    TABLE_NAME: str = "valuation_momentum"
    
    # API Settings
    HISTORICAL_LOOKBACK_DAYS: int = 400
    RECENT_DATA_AGE_LIMIT_DAYS: int = 1

    # Rate Limiting
    MAX_CONCURRENT_WORKERS: int = 3
    REQUESTS_PER_SECOND: float = 0.5  # 1 request per 2 seconds
    
    # Data Sources
    SP500_CSV_PATH: Path = RAW_DATA_DIR / "sp500.csv"
    NASDAQ_CSV_PATH: Path = RAW_DATA_DIR / "nasdaq.csv"

    # Crypto Settings
    CRYPTO_TABLE_NAME: str = "crypto_metrics"
    CRYPTO_MA_PERIOD: int = 20
    CRYPTO_DEFAULT_TIMEFRAME: str = "1h"
    CRYPTO_DATA_AGE_LIMIT_MINUTES: int = 5
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: Optional[Path] = BASE_DIR / "logs" / "trading.log"
    
    # Web App Settings
    WEB_APP_HOST: str = "127.0.0.1"
    WEB_APP_PORT: int = 8051
    WEB_APP_DEBUG: bool = True
    
    # --- General ---
    APP_NAME: str = "TradingGui"
    DEBUG: bool = False
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"

# Create global settings instance
settings = Settings()

# Ensure directories exist
settings.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
if settings.LOG_FILE:
    settings.LOG_FILE.parent.mkdir(parents=True, exist_ok=True) 