# src/ingestion/coingecko_client.py
import requests
import time
from datetime import datetime
import pandas as pd
from src.utils.logger import setup_logger
from src.config import Config

logger = setup_logger(__name__)

class CoinGeckoClient:
    def __init__(self):
        """
        Initializes the CoinGecko API client with rate limiting protection
        and error handling capabilities.
        """
        self.base_url = Config.COINGECKO_API_URL
        self.session = requests.Session()
        self.last_request_time = 0
        self.rate_limit_delay = 1.0  # Minimum seconds between requests
    
    def _make_request(self, endpoint, params=None):
        """
        Makes a rate-limited API request with error handling.
        
        Args:
            endpoint (str): API endpoint to call
            params (dict): Query parameters for the request
            
        Returns:
            dict: JSON response from the API
        """
        # Implement rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        if time_since_last_request < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last_request)
        
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
    
    def get_current_prices(self, coin_ids):
        """
        Fetches current prices for specified cryptocurrencies.
        
        Args:
            coin_ids (list): List of coin IDs to fetch prices for
            
        Returns:
            pandas.DataFrame: Current price data
        """
        try:
            response = self._make_request(
                'simple/price',
                params={
                    'ids': ','.join(coin_ids),
                    'vs_currencies': 'usd',
                    'include_market_cap': 'true',
                    'include_24hr_vol': 'true'
                }
            )
            
            # Transform response into a DataFrame
            records = []
            timestamp = datetime.utcnow()
            
            for coin_id, data in response.items():
                records.append({
                    'coin_id': coin_id,
                    'price_usd': data['usd'],
                    'market_cap_usd': data['usd_market_cap'],
                    'volume_24h_usd': data['usd_24h_vol'],
                    'timestamp': timestamp
                })
            
            return pd.DataFrame(records)
        except Exception as e:
            logger.error(f"Failed to fetch current prices: {str(e)}")
            raise