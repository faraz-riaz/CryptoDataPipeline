# run_consumer.py
from src.streaming.kafka_consumer import CryptoConsumer

consumer = CryptoConsumer()
consumer.process_messages()