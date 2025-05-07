from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, arrays_zip
import argparse
from datetime import datetime
import logging
import sys
from cleaning_utils import load_data_to_spark, _check_num_nulls


def nomnalize_columns(cleaned_df: DataFrame) -> DataFrame:
    # explode artists and artist_id columns into another DataFrame
    print(cleaned_df.show(10))

    df_1 = (
        cleaned_df.select("id", "name", "artists", "artist_ids")
        .withColumn("artists_info", explode(arrays_zip("artists", "artist_ids")))
        .select(
            "id",
            "name",
            col("artists_info.artists").alias("artist"),
            col("artists_info.artist_ids").alias("artist_id"),
        )
    )

    print(df_1.show(50))

    return df_1


def drop_dupes(cleaned_df: DataFrame, id_list):
    cols = cleaned_df.columns
    df = cleaned_df.join(id_list, cleaned_df.id == id_list.id, "left_anti").select(
        *cols
    )

    return df


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
            logging.FileHandler("load.log"),
        ],
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting Spark Load Pipeline - {current_time}")
    spark = SparkSession.builder.appName("Spark Load Pipeline").getOrCreate()
    s3_path = "s3a://spark-data/gold_data/song/latest/"
    file_path = args.file_path
    logger.info(f"File path: {file_path}")

    # Load data to Spark DataFrame
    try:
        df = load_data_to_spark(file_path, spark, data_type="gold")
        print(df.show(5))
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise e

    # Clean data

    artist_df = nomnalize_columns(df)

    # Load data to PostgreSQL
    connection_properties = {"user": args.db_user, "password": args.db_pass}
    connection_properties["driver"] = "org.postgresql.Driver"

    id_list = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/gold_data_db")
        .option("user", args.db_user)
        .option("password", args.db_pass)
        .option("driver", "org.postgresql.Driver")
        .option("query", "SELECT id FROM gold_table")
        .load()
    )

    df = drop_dupes(df, id_list)

    cols = df.columns
    cols.remove("release_date")
    df = process_nan(logger, cols, df)

    id_artist_list = (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/gold_data_db")
        .option("user", args.db_user)
        .option("password", args.db_pass)
        .option("driver", "org.postgresql.Driver")
        .option("query", "SELECT id FROM gold_artist_table")
        .load()
    )

    artist_df = drop_dupes(artist_df, id_artist_list)

    artist_df = process_nan(logger, artist_df.columns, artist_df)

    print(f"Number of rows to be inserted: {df.count()}")
    df.write.jdbc(
        "jdbc:postgresql://postgres:5432/gold_data_db",
        table="gold_table",
        mode="append",
        properties=connection_properties,
    )

    artist_df.write.jdbc(
        "jdbc:postgresql://postgres:5432/gold_data_db",
        table="gold_artist_table",
        mode="append",
        properties=connection_properties,
    )

    spark.stop()
    logger.info("Spark session stopped")
    logger.info("Pipeline completed")
    logger.info(f"End of Spark Raw Cleaning Pipeline - {current_time}")
