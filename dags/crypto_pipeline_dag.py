# dags/crypto_pipeline_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from src.ingestion.coingecko_client import CoinGeckoClient
from src.ingestion.data_validator import DataValidator
from src.storage.local_storage import LocalStorage

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

def fetch_and_store_data():
    client = CoinGeckoClient()
    validator = DataValidator()
    storage = LocalStorage()
    
    df = client.get_current_prices(Config.COINS_OF_INTEREST)
    validated_df = validator.validate_dataset(df)
    storage.store_data(validated_df)

with DAG(
    'crypto_pipeline',
    default_args=default_args,
    description='Cryptocurrency data pipeline',
    schedule_interval='*/5 * * * *',
    catchup=False
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_and_store_data',
        python_callable=fetch_and_store_data
    )