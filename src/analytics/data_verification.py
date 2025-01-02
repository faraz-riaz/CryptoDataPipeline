# src/analytics/data_verification.py
from google.cloud import bigquery
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def verify_data_pipeline():
    client = bigquery.Client()
    
    query = """
    SELECT 
        coin_id,
        COUNT(*) as record_count,
        MIN(timestamp) as earliest_record,
        MAX(timestamp) as latest_record,
        AVG(price_usd) as avg_price
    FROM crypto_data.price_data
    GROUP BY coin_id
    ORDER BY record_count DESC
    """
    
    try:
        df = client.query(query).to_dataframe()
        print("\nData Pipeline Verification:")
        print("==========================")
        print(df)
        return df
    except Exception as e:
        logger.error(f"Verification query failed: {str(e)}")
        raise

if __name__ == "__main__":
    verify_data_pipeline()