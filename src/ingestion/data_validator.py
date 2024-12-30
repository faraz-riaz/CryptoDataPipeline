# src/ingestion/data_validator.py
import pandas as pd
from datetime import datetime, timedelta
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class DataValidator:
    """
    Validates cryptocurrency price data to ensure quality and consistency.
    Performs checks for missing values, data types, and reasonable value ranges.
    """
    
    def __init__(self):
        # Define expected schema and validation rules
        self.expected_columns = {
            'coin_id': str,
            'price_usd': float,
            'market_cap_usd': float,
            'volume_24h_usd': float,
            'timestamp': datetime
        }
        
        # Define reasonable value ranges for numerical fields
        self.validation_rules = {
            'price_usd': {
                'min': 0,
                'max': 1e6  # Assuming no single coin will be worth more than $1M
            },
            'market_cap_usd': {
                'min': 0,
                'max': 1e15  # $1 quadrillion upper limit
            },
            'volume_24h_usd': {
                'min': 0,
                'max': 1e12  # $1 trillion daily volume upper limit
            }
        }

    def validate_schema(self, df):
        """
        Validates that the DataFrame has the expected columns and data types.
        """
        # Check for missing columns
        missing_columns = set(self.expected_columns.keys()) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Validate data types
        for column, expected_type in self.expected_columns.items():
            if column == 'timestamp' and not pd.api.types.is_datetime64_any_dtype(df[column]):
                try:
                    df[column] = pd.to_datetime(df[column])
                except Exception as e:
                    raise ValueError(f"Could not convert timestamp column to datetime: {str(e)}")
            elif column != 'timestamp' and df[column].dtype != expected_type:
                try:
                    df[column] = df[column].astype(expected_type)
                except Exception as e:
                    raise ValueError(f"Could not convert {column} to {expected_type}: {str(e)}")
        
        return df

    def validate_values(self, df):
        """
        Validates that numerical values fall within expected ranges and handles missing values.
        """
        # Check for missing values
        missing_counts = df.isnull().sum()
        if missing_counts.any():
            logger.warning(f"Found missing values:\n{missing_counts[missing_counts > 0]}")
            raise ValueError("Dataset contains missing values")

        # Validate numerical ranges
        for column, rules in self.validation_rules.items():
            out_of_range = df[
                (df[column] < rules['min']) | (df[column] > rules['max'])
            ]
            
            if not out_of_range.empty:
                logger.error(f"Found {len(out_of_range)} values out of range for {column}")
                raise ValueError(f"Values out of acceptable range in column {column}")

        # Validate timestamps
        current_time = datetime.utcnow()
        time_window = current_time - timedelta(days=1)
        
        future_timestamps = df[df['timestamp'] > current_time]
        if not future_timestamps.empty:
            raise ValueError("Found timestamps in the future")

        return df

    def validate_dataset(self, df):
        """
        Performs all validation checks on the dataset.
        """
        try:
            df = self.validate_schema(df)
            df = self.validate_values(df)
            logger.info("Data validation completed successfully")
            return df
        except Exception as e:
            logger.error(f"Data validation failed: {str(e)}")
            raise