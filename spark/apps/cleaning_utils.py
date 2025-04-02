import logging
import os
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    udf,
    round,
    expr,
    to_date,
    split,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
)


def check_s3_path(s3_path: str) -> bool:
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"From {__name__} - check_s3_path at {current_time}")

    try:
        if os.path.exists(s3_path):
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error checking S3 path {s3_path}: {e}")
        return False


def _get_schema():
    my_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("album", StringType(), True),
            StructField("album_id", StringType(), True),
            StructField(
                "artists", StringType(), True
            ),  # The original data is in csv, which does not support array type
            StructField("artist_id", StringType(), True),  # The same as above
            StructField("track_number", IntegerType(), True),
            StructField("disc_number", IntegerType(), True),
            StructField("explicit", StringType(), True),
            StructField("danceability", FloatType(), True),
            StructField("energy", FloatType(), True),
            StructField("key", IntegerType(), True),
            StructField("loudness", FloatType(), True),
            StructField("mode", IntegerType(), True),
            StructField("speechiness", FloatType(), True),
            StructField("acousticness", FloatType(), True),
            StructField("instrumentalness", FloatType(), True),
            StructField("liveness", FloatType(), True),
            StructField("valence", FloatType(), True),
            StructField("tempo", FloatType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("time_signature", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("release_date", StringType(), True),
        ]
    )

    return my_schema


def _get_blank_df(spark: SparkSession) -> DataFrame:
    my_schema = _get_schema()
    blank_df = spark.createDataFrame([], my_schema)
    return blank_df


def load_data_to_spark(s3_path: str, spark: SparkSession) -> None:
    schema = _get_schema()
    df = spark.read.csv(s3_path, header=True, schema=schema)
    return df


def _clean_name(df: DataFrame, col_name="name"):
    c = lower(trim(col(col_name)))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    c = regexp_replace(c, regex_str, None)
    c = c.alias("cleaned_" + col_name)
    return_col = df.select(c)
    return return_col


def _clean_album(df: DataFrame, col_name="album"):
    c = lower(trim(col(col_name)))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    c = regexp_replace(c, regex_str, None)
    c = c.alias("cleaned_" + col_name)
    return_col = df.select(c)
    return return_col


def __clean_artist_elements(lst):
    for artist in lst:
        if artist == "" or artist is None or artist == "nan" or artist == "null":
            artist = None
        else:
            artist = artist.strip().lower()
    return lst


def __clean_artist_id_elements(lst):
    for i in lst:
        if i == "" or i is None or i == "nan" or i == "null":
            i = None
        else:
            i = i.strip()
    return lst


def _clean_artists(df: DataFrame, col_name="artists"):
    c = col(col_name)
    df = df.withColumn(
        col_name,
        split(c, r","),
    )
    helper_func = udf(__clean_artist_elements, ArrayType(StringType()))
    c = helper_func(c).alias("cleaned_" + col_name)
    return_col = df.select(c)
    return return_col


def _clean_artist_id(df: DataFrame, col_name="artist_id"):
    c = col(col_name)
    df = df.withColumn(
        col_name,
        split(c, r","),
    )
    helper_func = udf(__clean_artist_id_elements, ArrayType(StringType()))
    c = helper_func(c).alias("cleaned_" + col_name)
    return_col = df.select(c)
    return return_col


def _clean_explicit(df: DataFrame, col_name="explicit"):
    c = col(col_name)
    c = lower(trim(c))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    c = regexp_replace(c, regex_str, None)
    c = regexp_replace(c, "true", "True")
    c = regexp_replace(c, "false", "False")
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_danceability(df: DataFrame, col_name="danceability"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_energy(df: DataFrame, col_name="energy"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_loudness(df: DataFrame, col_name="loudness"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_mode(df: DataFrame, col_name="mode"):
    c = col(col_name).cast(StringType())
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    c = regexp_replace(c, regex_str, None)
    c = regexp_replace(c, "0", "Minor")
    c = regexp_replace(c, "1", "Major")
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_speechiness(df: DataFrame, col_name="speechiness"):
    c = col(col_name)
    c = round(c, 4)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_acousticness(df: DataFrame, col_name="acousticness"):
    c = col(col_name)
    c = round(c, 4)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_instrumentalness(df: DataFrame, col_name="instrumentalness"):
    c = col(col_name)
    c = round(c, 4)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_liveness(df: DataFrame, col_name="liveness"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_valence(df: DataFrame, col_name="valence"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_tempo(df: DataFrame, col_name="tempo"):
    c = col(col_name)
    c = round(c, 3)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col


def _clean_duration_ms(df: DataFrame, col_name="duration_ms"):
    c = col(col_name)
    c = expr(c / 1000)
    return_col = df.select(c.alias("cleaned_duration_s"))
    return return_col


def _clean_release_date(df: DataFrame, col_name="release_date"):
    date_format = "MM/dd/yyyy"
    c = col(col_name)
    c = to_date(c, date_format)
    return_col = df.select(c.alias("cleaned_" + col_name))
    return return_col
