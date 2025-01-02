# src/streaming/kafka_consumer.py
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
from src.config import Config
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class CryptoConsumer:
    def __init__(self):
        """Initialize Kafka consumer with configuration"""
        self.consumer = KafkaConsumer(
            Config.KAFKA_TOPIC,
            bootstrap_servers=[Config.KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        self.previous_prices = {}
        logger.info("Kafka consumer initialized")
    
    def calculate_price_changes(self, current_data):
        """
        Calculate price changes and detect significant movements
        
        Args:
            current_data (dict): Current price data
        """
        coin_id = current_data['coin_id']
        current_price = current_data['price_usd']
        
        if coin_id in self.previous_prices:
            previous_price = self.previous_prices[coin_id]
            price_change = ((current_price - previous_price) / previous_price) * 100
            
            # Check if price change exceeds threshold
            if abs(price_change) >= Config.PRICE_CHANGE_THRESHOLD:
                logger.info(f"Significant price movement for {coin_id}: {price_change:.2f}%")
                self.handle_significant_movement(coin_id, price_change, current_price)
        
        # Update previous price
        self.previous_prices[coin_id] = current_price
    
    def handle_significant_movement(self, coin_id, price_change, current_price):
        """
        Handle significant price movements
        
        Args:
            coin_id (str): Cryptocurrency identifier
            price_change (float): Percentage price change
            current_price (float): Current price
        """
        movement_type = "increase" if price_change > 0 else "decrease"
        
        alert = {
            'timestamp': datetime.utcnow().isoformat(),
            'coin_id': coin_id,
            'price_change': price_change,
            'current_price': current_price,
            'movement_type': movement_type
        }
        
        # In a real application, you might:
        # - Send notifications
        # - Store alerts in a database
        # - Trigger trading signals
        logger.info(f"Alert generated: {alert}")
    
    def process_messages(self):
        """Main loop to process incoming Kafka messages"""
        logger.info("Starting to process messages...")
        
        try:
            for message in self.consumer:
                try:
                    # Convert message value to DataFrame
                    records = message.value
                    
                    # Process each record
                    for record in records:
                        self.calculate_price_changes(record)
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    continue
                    
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
            raise
        finally:
            self.consumer.close()