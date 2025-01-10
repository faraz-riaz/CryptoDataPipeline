from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max, min, count
from datetime import datetime, timedelta
from pyspark.sql.functions import col, window, avg, max, min, count
from datetime import datetime, timedelta
from src.utils.logger import setup_logger
from dotenv import load_dotenv

load_dotenv()
from dotenv import load_dotenv

load_dotenv()

logger = setup_logger(__name__)

class SparkProcessor:
    def __init__(self, local_mode=False):
        
        self.dataset_id = "crypto_data"
        self.table_id = "price_data"
        
        if local_mode:
            builder = SparkSession.builder.appName("CryptoAnalysis")
            builder = builder.master("local[*]")
            self.spark = builder.getOrCreate()
        else:
            self.spark = SparkSession.builder \
                .appName("CryptoAnalysis") \
                .config("spark.driver.memory", "4g") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "2") \
                .config("spark.driver.maxResultSize", "2g") \
                .config("spark.network.timeout", "800s") \
                .config("spark.executor.heartbeatInterval", "60s") \
                .getOrCreate()
        
        
    def analyze_historical_data(self):
        """Analyze cryptocurrency historical data from BigQuery"""
        try:
            # Read from BigQuery
            query = """
            SELECT coin_id, timestamp, price_usd, volume_24h_usd, market_cap_usd
            FROM crypto_data.price_data
            WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            """
                
            df = self.spark.read.format("bigquery") \
            .option("table", query) \
            .option("viewsEnabled", "true") \
            .option("materializationDataset", self.dataset_id) \
            .load()
            
            # Calculate daily metrics
            daily_metrics = df.groupBy(
                "coin_id",
                window("timestamp", "1 day")
            ).agg(
                avg("price_usd").alias("avg_price"),
                max("price_usd").alias("max_price"),
                min("price_usd").alias("min_price"),
                avg("volume_24h_usd").alias("avg_volume"),
                avg("market_cap_usd").alias("avg_market_cap"),
                count("*").alias("data_points")
            )
            
            # Calculate volatility metrics
            volatility = df.groupBy("coin_id").agg(
                ((max("price_usd") - min("price_usd")) / avg("price_usd")).alias("volatility")
            )
            # Calculate daily metrics
            daily_metrics = df.groupBy(
                "coin_id",
                window("timestamp", "1 day")
            ).agg(
                avg("price_usd").alias("avg_price"),
                max("price_usd").alias("max_price"),
                min("price_usd").alias("min_price"),
                avg("volume_24h_usd").alias("avg_volume"),
                avg("market_cap_usd").alias("avg_market_cap"),
                count("*").alias("data_points")
            )
            
            # Calculate volatility metrics
            volatility = df.groupBy("coin_id").agg(
                ((max("price_usd") - min("price_usd")) / avg("price_usd")).alias("volatility")
            )
            
            return daily_metrics, volatility
            return daily_metrics, volatility
            
        except Exception as e:
            logger.error(f"Spark session error: {str(e)}")
            # Attempt to recover
            self.spark.stop()
            self.__init__()  # Reinitialize Spark session
            raise

    def save_metrics(self, daily_metrics, volatility):
        """Save computed metrics to BigQuery"""
        try:
            daily_metrics.write \
                .format("bigquery") \
                .option("table", "crypto_data.daily_metrics") \
                .mode("append") \
                .save()
                
            volatility.write \
                .format("bigquery") \
                .option("table", "crypto_data.volatility_metrics") \
                .mode("append") \
                .save()
                
        except Exception as e:
            logger.error(f"Failed to save metrics: {str(e)}")
            raise