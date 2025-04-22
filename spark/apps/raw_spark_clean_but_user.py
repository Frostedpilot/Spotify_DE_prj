from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import argparse
from datetime import datetime
import logging
import sys
from cleaning_utils_but_user import (
    load_data_to_spark,
    _clean_album_artist,
    _clean_ts,
    _clean_track_name,
    _clean_album_name,
    _clean_ms_played,
    _clean_platform,
    _clean_type,
    _clean_id,
)


def spark_clean(logger, df: DataFrame):
    # Start the cleaning process
    logger.info("Starting data cleaning process")
    logger.info(f"DataFrame schema before cleaning: {df.printSchema()}")
    logger.info(f"DataFrame count before cleaning: {df.count()}")
    logger.info(f"DataFrame columns before cleaning: {df.columns}")

    # ts column cleaning
    logger.info("Cleaning ts column")
    ts = _clean_ts(df, col_name="ts").alias("ts")

    # platform column cleaning
    logger.info("Cleaning platform column")
    platform = _clean_platform(df, col_name="platform").alias("platform")

    # ms_played column cleaning
    logger.info("Cleaning ms_played column")
    ms_played = _clean_ms_played(df, col_name="ms_played").alias("seconds_played")

    # master_metadata_track_name column cleaning
    logger.info("Cleaning master_metadata_track_name column")
    master_metadata_track_name = _clean_track_name(
        df, col_name="master_metadata_track_name"
    ).alias("track_name")

    # master_metadata_album_artist_name column cleaning
    logger.info("Cleaning master_metadata_album_artist_name column")
    master_metadata_album_artist_name = _clean_album_artist(
        df, col_name="master_metadata_album_artist_name"
    ).alias("album_artist_name")

    # master_metadata_album_name column cleaning
    logger.info("Cleaning master_metadata_album_album_name column")
    master_metadata_album_name = _clean_album_name(
        df, col_name="master_metadata_album_album_name"
    ).alias("album_name")

    # type column cleaning
    logger.info("Cleaning type column")
    track_type = (
        _clean_type(df, col_name="spotify_track_uri").alias("type").alias("type")
    )

    # no need to clean spotify_track_uri column
    logger.info("Cleaning spotify_track_uri column")
    track_id = _clean_id(df, col_name="spotify_track_uri").alias("id")

    # All columns cleaned, return them
    logger.info("All columns cleaned successfully")
    col_lst = [
        track_id,
        ts,
        platform,
        ms_played,
        master_metadata_track_name,
        master_metadata_album_artist_name,
        master_metadata_album_name,
        track_type,
    ]

    logger.info(f"Columns to be selected: {col_lst}")
    return col_lst


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
    logger.info(f"Starting User Spark Raw Cleaning Pipeline - {current_time}")
    spark = SparkSession.builder.appName(
        "User Spark Raw Cleaning Pipeline"
    ).getOrCreate()
    s3_path = "s3a://spark-data/raw_uploaded_csvs/user/latest/"
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

    col_lst = spark_clean(logger, df)
    logger.info("Data cleaning process completed successfully")

    df_cleaned = df.select(*col_lst)
    print(df_cleaned.show(5))

    logger.info(f"DataFrame schema after cleaning: {df_cleaned.printSchema()}")
    logger.info(f"DataFrame count after cleaning: {df_cleaned.count()}")
    logger.info(f"DataFrame columns after cleaning: {df_cleaned.columns}")

    # Save cleaned DataFrame to S3
    s3_output_path = "s3a://spark-data/gold_data/user/latest/"
    logger.info(f"Saving cleaned DataFrame parquet to {s3_output_path}")
    try:
        df_cleaned.write.mode("overwrite").parquet(s3_output_path)
        logger.info("Data saved successfully")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise e

    # Save cleaned DataFrame to PostgreSQL
    connection_properties = {"user": args.db_user, "password": args.db_pass}
    connection_properties["driver"] = "org.postgresql.Driver"

    print(f"Number of rows to be inserted: {df.count()}")
    df_cleaned.write.jdbc(
        "jdbc:postgresql://postgres:5432/gold_data_db",
        table="gold_user_table",
        mode="append",
        properties=connection_properties,
    )

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Pipeline completed")
    logger.info(f"End of Spark Raw Cleaning Pipeline - {current_time}")
