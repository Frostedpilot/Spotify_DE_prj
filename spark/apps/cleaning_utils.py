import logging
import os
from datetime import datetime, timedelta
from pyspark.sql.functions import col

def load_data_to_spark(s3_path: str, spark) -> None:
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"From {__name__} - load_data_to_spark at {current_time}")

    try:
        logger.info(f"Reading data from {s3_path}")
        df = spark.read.csv(s3_path, header=True, inferSchema=True)
        logger.info(f"Data loaded successfully from {s3_path}")
    except Exception as e:
        logger.error(f"Error loading data from {s3_path}: {e}")
        raise e

def _clean_name(df, col_name='name'):
    df = df.withColumn(col_name, col(col_name).cast("string"))
    df = df.withColumn(col_name, col(col_name).strip())