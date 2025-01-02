# src/storage/bigquery_storage.py
from google.cloud import bigquery
import pandas as pd
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class BigQueryStorage:
    def __init__(self):
        self.client = bigquery.Client()
        self.dataset_id = "crypto_data"
        self.table_id = "price_data"
        self._setup_dataset()
        
    def _setup_dataset(self):
        """Create dataset and table if they don't exist"""
        dataset_ref = self.client.dataset(self.dataset_id)
        
        try:
            self.client.get_dataset(dataset_ref)
        except Exception:
            # Dataset doesn't exist, create it
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            
            # Create table schema
            schema = [
                bigquery.SchemaField("coin_id", "STRING"),
                bigquery.SchemaField("price_usd", "FLOAT"),
                bigquery.SchemaField("market_cap_usd", "FLOAT"),
                bigquery.SchemaField("volume_24h_usd", "FLOAT"),
                bigquery.SchemaField("timestamp", "TIMESTAMP")
            ]
            
            table_ref = dataset_ref.table(self.table_id)
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            
    def store_data(self, df):
        """Store DataFrame in BigQuery"""
        table_ref = f"{self.dataset_id}.{self.table_id}"
        
        try:
            df.to_gbq(
                destination_table=table_ref,
                project_id=self.client.project,
                if_exists='append'
            )
            logger.info(f"Successfully stored {len(df)} rows in BigQuery")
        except Exception as e:
            logger.error(f"Failed to store data in BigQuery: {str(e)}")
            raise