from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, arrays_zip
import argparse
from datetime import datetime
import logging
import sys
from cleaning_utils import load_data_to_spark, _check_num_nulls


def process_nan(logger, col_names, df: DataFrame):
    rate_nulls, num_nulls, num_rows = _check_num_nulls(df, col_names)

    if rate_nulls < 0.1 or num_nulls <= 10:
        logger.info(f"Null values in the DataFrame: {num_nulls} ({rate_nulls:.2%})")
        logger.info(
            "DataFrame does not contain too many null values, dropping all nulls"
        )

        print(f"Null values in the DataFrame: {num_nulls} ({rate_nulls:.2%})")

        df = df.dropna()
    else:
        logger.info(f"Null values in the DataFrame: {num_nulls} ({rate_nulls:.2%})")
        logger.info(
            "DataFrame contains too many null values, cleaning them and not dropping"
        )

        logger.error("Not yet implemented")

        raise NotImplementedError(
            "Cleaning with too many null values is not yet implemented"
        )

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True)
    parser.add_argument("--gold_db", required=True)
    parser.add_argument("--db_user", required=True)
    parser.add_argument("--db_pass", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("silver_spark_clean.log"),
        ],
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting Spark Silver Cleaning Pipeline - {current_time}")
    spark = SparkSession.builder.appName("Spark Silver Cleaning Pipeline").getOrCreate()
    s3_path = "s3a://spark-data/silver_data/song/latest/"
    file_path = args.file_path
    logger.info(f"File path: {file_path}")

    # Load data to Spark DataFrame
    try:
        df = load_data_to_spark(file_path, spark, data_type="silver")
        print(df.show(5))
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise e

    # Clean data
    col_names = df.columns
    col_names.remove("release_date")

    df = process_nan(logger, col_names, df)

    # Save to s3
    s3_output_path = "s3a://spark-data/gold_data/song/latest/"

    try:
        df.write.mode("overwrite").parquet(s3_output_path)
        logger.info("Data saved successfully")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise e

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Pipeline completed")
    logger.info(f"End of Spark Raw Cleaning Pipeline - {current_time}")
