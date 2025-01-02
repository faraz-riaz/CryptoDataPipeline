# src/batch/spark_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class SparkProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("CryptoAnalysis") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .getOrCreate()
    
    def analyze_historical_data(self, input_path):
        """
        Analyze historical cryptocurrency data using Spark
        
        Args:
            input_path (str): Path to parquet files
        """
        try:
            # Read parquet files
            df = self.spark.read.parquet(input_path)
            
            # Daily price analysis
            daily_metrics = df.groupBy("coin_id") \
                .agg(
                    avg("price_usd").alias("avg_price"),
                    max("price_usd").alias("max_price"),
                    min("price_usd").alias("min_price")
                )
            
            logger.info("Computed daily metrics:")
            daily_metrics.show()
            
            return daily_metrics
            
        except Exception as e:
            logger.error(f"Spark processing failed: {str(e)}")
            raise