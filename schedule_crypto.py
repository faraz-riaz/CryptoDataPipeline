# schedule_crypto.py
import schedule
import time
from src.ingestion.coingecko_client import CoinGeckoClient
from src.ingestion.data_validator import DataValidator
from src.storage.local_storage import LocalStorage
from src.config import Config

def job():
    client = CoinGeckoClient()
    validator = DataValidator()
    storage = LocalStorage()
    
    df = client.get_current_prices(Config.COINS_OF_INTEREST)
    validated_df = validator.validate_dataset(df)
    storage.store_data(validated_df)

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)