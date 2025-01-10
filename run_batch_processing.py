# run_batch_processing.py
from src.batch.spark_processor import SparkProcessor
from src.config import Config
from src.utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger(__name__)

def main():
    """
    Run the batch processing Spark job.
    """
    try:
        processor = SparkProcessor(local_mode=True)
        daily_metrics, volatility = processor.analyze_historical_data()
        processor.save_metrics(daily_metrics, volatility)
    except Exception as e:
        logger.error(f"Error running batch processing: {str(e)}")

if __name__ == "__main__":
    main()
