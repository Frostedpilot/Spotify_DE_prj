from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import argparse
from datetime import datetime
import logging
import sys
from cleaning_utils import load_data_to_spark, _check_num_nulls


def clean_silver(logger, df: DataFrame):
    col_names = df.columns
    # Remove the release_date column from the list of columns since it is not really needed

    col_names.remove("release_date")

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
        stream=sys.stdout,
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting Spark Silver Cleaning Pipeline - {current_time}")
    spark = SparkSession.builder.appName("Spark Silver Cleaning Pipeline").getOrCreate()
    s3_path = "s3a://spark-data/silver_data/latest/"
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
    df = clean_silver(logger, df)
    print(df.show(5))

    # Save to s3
    s3_output_path = "s3a://spark-data/gold_data/latest/"

    try:
        df.write.mode("overwrite").parquet(s3_output_path)
        logger.info("Data saved successfully")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise e

    # Load data to PostgreSQL
    connection_properties = {"user": args.db_user, "password": args.db_pass}
    connection_properties["driver"] = "org.postgresql.Driver"

    print(f"Number of rows to be inserted: {df.count()}")
    df.write.jdbc(
        "jdbc:postgresql://postgres:5432/gold_data_db",
        table="gold_table",
        mode="overwrite",
        properties=connection_properties,
    )
