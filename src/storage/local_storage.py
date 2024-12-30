# src/storage/local_storage.py
import os
import pandas as pd
from datetime import datetime
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class LocalStorage:
    """
    Handles local storage of cryptocurrency data using parquet files.
    Data is organized by date to make it easier to manage and query.
    """
    
    def __init__(self, base_path="data"):
        """
        Initialize storage with a base directory for data files.
        
        Args:
            base_path (str): Base directory for storing data files
        """
        self.base_path = base_path
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Creates necessary directories if they don't exist."""
        os.makedirs(self.base_path, exist_ok=True)
    
    def _get_daily_path(self, timestamp):
        """
        Gets the path for storing data for a specific date.
        
        Args:
            timestamp (datetime): Timestamp to generate path for
            
        Returns:
            str: Path where data should be stored
        """
        # Organize files by year/month/day
        date_path = timestamp.strftime("%Y/%m/%d")
        return os.path.join(self.base_path, date_path)
    
    def store_data(self, df):
        """
        Stores DataFrame to parquet files, organized by date.
        
        Args:
            df (pandas.DataFrame): Data to store
        """
        if df.empty:
            logger.warning("Received empty DataFrame, skipping storage")
            return
        
        # Get current timestamp for file naming
        current_time = datetime.utcnow()
        
        # Create directory path for current date
        daily_path = self._get_daily_path(current_time)
        os.makedirs(daily_path, exist_ok=True)
        
        # Generate filename with timestamp
        filename = f"crypto_data_{current_time.strftime('%H%M%S')}.parquet"
        full_path = os.path.join(daily_path, filename)
        
        try:
            # Store data in parquet format
            df.to_parquet(full_path, index=False)
            logger.info(f"Successfully stored data to {full_path}")
            
            # Also update latest snapshot for quick access
            latest_path = os.path.join(self.base_path, "latest.parquet")
            df.to_parquet(latest_path, index=False)
            
            return full_path
        except Exception as e:
            logger.error(f"Failed to store data: {str(e)}")
            raise
    
    def read_latest_data(self):
        """
        Reads the most recent data snapshot.
        
        Returns:
            pandas.DataFrame: Most recent data, or empty DataFrame if no data exists
        """
        latest_path = os.path.join(self.base_path, "latest.parquet")
        try:
            if os.path.exists(latest_path):
                return pd.read_parquet(latest_path)
            else:
                logger.warning("No latest data found")
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Failed to read latest data: {str(e)}")
            raise