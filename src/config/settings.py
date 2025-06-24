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
    YAHOO_FIN_RATE_LIMIT: int = 1 # seconds between requests
    
    # Data Sources
    SP500_CSV_PATH: Path = RAW_DATA_DIR / "sp500.csv"
    NASDAQ_CSV_PATH: Path = RAW_DATA_DIR / "nasdaq.csv"
    
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

# Create global settings instance
settings = Settings()

# Ensure directories exist
settings.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
settings.PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
if settings.LOG_FILE:
    settings.LOG_FILE.parent.mkdir(parents=True, exist_ok=True) 