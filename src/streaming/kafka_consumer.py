# src/streaming/kafka_consumer.py
from kafka import KafkaConsumer
import json
import pandas as pd

class CryptoConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'crypto-prices',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
    
    def process_messages(self):
        for message in self.consumer:
            data = pd.DataFrame(message.value)
            # Calculate real-time metrics
            self.calculate_metrics(data)
    
    def calculate_metrics(self, data):
        # Calculate 5-minute price changes
        previous_data = pd.read_parquet('data/latest.parquet')
        price_change = ((data['price_usd'] - previous_data['price_usd']) / 
                       previous_data['price_usd'] * 100)
        print(f"Price changes: {price_change}")