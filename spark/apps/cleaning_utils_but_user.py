import logging
import os
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_extract,
    round,
    expr,
    to_timestamp,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    FloatType,
    ArrayType,
    MapType,
    TimestampType,
    DateType,
    DecimalType,
    BinaryType,
    BooleanType,
)


def _get_schema():
    my_schema = StructType(
        [
            StructField("ts", StringType(), True),
            StructField("platform", StringType(), True),
            StructField("ms_played", IntegerType(), True),
            StructField("conn_country", StringType(), True),
            StructField("master_metadata_track_name", StringType(), True),
            StructField("master_metadata_album_artist_name", StringType(), True),
            StructField("master_metadata_album_name", StringType(), True),
            StructField("spotify_track_uri", StringType(), True),
            StructField("episode_name", StringType(), True),
            StructField("episode_show_name", StringType(), True),
            StructField("spotify_episode_uri", StringType(), True),
            StructField("audiobook_title", StringType(), True),
            StructField("audiobook_uri", StringType(), True),
            StructField("audiobook_chapter_uri", StringType(), True),
            StructField("audiobook_chapter_title", StringType(), True),
            StructField("reason_start", StringType(), True),
            StructField("reason_end", StringType(), True),
            StructField("shuffle", BooleanType(), True),
            StructField("skipped", BooleanType(), True),
            StructField("offline", StringType(), True),
            StructField("offline_timestamp", LongType(), True),
            StructField("incognito_mode", BooleanType(), True),
        ]
    )

    return my_schema


def _get_blank_df(spark: SparkSession) -> DataFrame:
    my_schema = _get_schema()
    blank_df = spark.createDataFrame([], my_schema)
    return blank_df


def load_data_to_spark(s3_path: str, spark: SparkSession) -> None:
    schema = _get_schema()
    df = spark.read.parquet(s3_path, header=True, schema=schema)
    return df


def _clean_ts(df: DataFrame, col_name: str = "ts") -> col:
    c = to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    return c


def _clean_platform(df: DataFrame, col_name: str = "platform") -> col:
    platforms = ["android", "windows", "ios", "web"]
    c = trim(lower(col(col_name)))
    extracted_c = regexp_extract(c, "(" + "|".join(platforms) + ")", 1)
    return extracted_c


def _clean_ms_played(df: DataFrame, col_name: str = "ms_played") -> col:
    c = expr(f"{col_name} / 1000")
    rounded = round(c).cast("integer")
    return rounded


def _clean_track_name(df: DataFrame, col_name: str = "master_metadata_track_name"):
    return col(col_name).alias("track_name")


def _clean_album_artist(
    df: DataFrame, col_name: str = "master_metadata_album_artist_name"
):
    return col(col_name).alias("album_artist_name")


def _clean_album_name(df: DataFrame, col_name: str = "master_metadata_album_name"):
    return col(col_name).alias("album_name")


def _clean_type(df: DataFrame, col_name: str = "spotify_track_uri"):
    c = regexp_extract(col(col_name), r"\w*:(\w*):", 1)
    return c


def _clean_id(df: DataFrame, col_name: str = "spotify_track_uri"):
    c = regexp_extract(col(col_name), r"\w*:\w*:(\w*)", 1)
    return c
