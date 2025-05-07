from pyspark.sql import SparkSession, DataFrame
import argparse
from datetime import datetime
import logging
import sys
from great_expectations.core.batch import RuntimeBatchRequest
from cleaning_utils_but_user import (
    load_data_to_spark,
)

# -- config -- #
CONTEXT_DIR = "/opt/airflow/great_expectations"
DATASOURCE_NAME = "spark_runtime_source"
DATA_CONNECTOR_NAME = "runtime_connector"
DATA_ASSET_NAME = "my_spark_dataframe_asset"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("user_spark_clean.log"),
        ],
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting GX Pipeline - {current_time}")
    spark = SparkSession.builder.appName("GX").getOrCreate()
    file_path = args.file_path
    logger.info(f"File path: {file_path}")

    # Load data to Spark DataFrame
    try:
        df = load_data_to_spark(file_path, spark)
        print(df.show(5))
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise e
