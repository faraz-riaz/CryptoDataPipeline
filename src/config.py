# src/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API Configuration
    COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
    COINS_OF_INTEREST = ["bitcoin", "ethereum", "cardano", "solana", "polkadot"]
    
    # Data Collection Settings
    HISTORICAL_DAYS = 365
    UPDATE_INTERVAL = 300  # 5 minutes in seconds
    
    # Logging Configuration
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_FILE = "logs/crypto_pipeline.log"