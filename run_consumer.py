# run_consumer.py
from src.streaming.kafka_consumer import CryptoConsumer
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def main():
    """
    Main function to run the Kafka consumer
    """
    try:
        consumer = CryptoConsumer()
        logger.info("Starting Kafka consumer...")
        consumer.process_messages()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.error(f"Error running consumer: {str(e)}")

if __name__ == "__main__":
    main()