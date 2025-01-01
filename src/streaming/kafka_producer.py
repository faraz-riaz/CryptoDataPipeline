# src/streaming/kafka_producer.py
from kafka import KafkaProducer
import json

class CryptoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = 'crypto-prices'

    def send_data(self, data):
        self.producer.send(self.topic, data.to_dict('records'))
        self.producer.flush()