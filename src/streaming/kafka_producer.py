# src/streaming/kafka_producer.py
from kafka import KafkaProducer
import json
from datetime import datetime
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class CryptoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v, default=self._datetime_handler).encode('utf-8')
        )
        self.topic = 'crypto-prices'

    def _datetime_handler(self, obj):
        """Handle datetime serialization to JSON"""
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def send_data(self, data):
        """
        Send data to Kafka topic
        Args:
            data (pandas.DataFrame): Data to send
        """
        try:
            # Convert DataFrame to dict and ensure timestamps are serializable
            records = data.to_dict('records')
            
            # Convert any timestamp objects to strings
            for record in records:
                if 'timestamp' in record:
                    record['timestamp'] = record['timestamp'].isoformat()

            self.producer.send(self.topic, records)
            self.producer.flush()
            logger.info(f"Successfully sent {len(records)} records to Kafka")
        except Exception as e:
            logger.error(f"Failed to send data to Kafka: {str(e)}")
            raise