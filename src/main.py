# src/main.py
import time
from src.ingestion.coingecko_client import CoinGeckoClient
from src.ingestion.data_validator import DataValidator
from src.storage.local_storage import LocalStorage
from src.streaming.kafka_producer import CryptoProducer
from src.config import Config
from src.utils.logger import setup_logger
from src.storage.bigquery_storage import BigQueryStorage

from dotenv import load_dotenv
load_dotenv()

logger = setup_logger(__name__)

def main():
    """
    Main function to run the data pipeline
    """
    client = CoinGeckoClient()
    validator = DataValidator()
    storage = LocalStorage()
    bq_storage = BigQueryStorage()
    producer = CryptoProducer()
    
    while True:
        try:
            # Fetch current prices
            logger.info("Fetching current prices...")
            df = client.get_current_prices(Config.COINS_OF_INTEREST)
            
            # Validate the data
            logger.info("Validating data...")
            validated_df = validator.validate_dataset(df)
            
            # Store the validated data
            logger.info("Storing data...")
            storage.store_data(validated_df)
            bq_storage.store_data(validated_df)
            
            # Send to Kafka
            logger.info("Sending to Kafka...")
            producer.send_data(validated_df)
            
            logger.info(f"Successfully processed {len(validated_df)} records")
            
            # Wait for next update interval
            time.sleep(Config.UPDATE_INTERVAL)
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            time.sleep(60)  # Wait a minute before retrying

if __name__ == "__main__":
    main()