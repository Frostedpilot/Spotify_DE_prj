import logging
import os
from datetime import datetime
from pyspark.sql.functions import (
    col,
    lower,
    trim,
    regexp_replace,
    round,
    expr,
    to_date,
    split,
    lit,
    transform,
    count,
    when,
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    ArrayType,
    MapType,
    TimestampType,
    DateType,
    DecimalType,
    BinaryType,
    BooleanType,
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
            StructField("artist_ids", StringType(), True),  # The same as above
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


def _get_silver_schema():
    my_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("album", StringType(), True),
            StructField("album_id", StringType(), True),
            StructField("artists", ArrayType(StringType()), True),
            StructField("artist_ids", ArrayType(StringType()), True),
            StructField("track_number", IntegerType(), True),
            StructField("disc_number", IntegerType(), True),
            StructField("explicit", BooleanType(), True),
            StructField("danceability", FloatType(), True),
            StructField("energy", FloatType(), True),
            StructField("key", IntegerType(), True),
            StructField("loudness", FloatType(), True),
            StructField("mode", StringType(), True),
            StructField("speechiness", FloatType(), True),
            StructField("acousticness", FloatType(), True),
            StructField("instrumentalness", FloatType(), True),
            StructField("liveness", FloatType(), True),
            StructField("valence", FloatType(), True),
            StructField("tempo", FloatType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("time_signature", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("release_date", DateType(), True),
        ]
    )

    return my_schema


def _get_blank_df(spark: SparkSession, data_type="raw") -> DataFrame:
    match data_type:
        case "raw":
            my_schema = _get_schema()
        case "silver":
            my_schema = _get_silver_schema()
        case _:
            raise ValueError(f"Unknown data type: {data_type}")
    blank_df = spark.createDataFrame([], my_schema)
    return blank_df


def load_data_to_spark(s3_path: str, spark: SparkSession, data_type="raw") -> None:
    match data_type:
        case "raw":
            schema = _get_schema()
        case "silver":
            schema = _get_silver_schema()
        case _:
            raise ValueError(f"Unknown data type: {data_type}")

    df = spark.read.parquet(s3_path, header=True, schema=schema)
    return df


def _clean_name(df: DataFrame, col_name="name"):
    c = lower(trim(col(col_name)))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    cleaned = regexp_replace(c, regex_str, "")
    return_col = cleaned.alias(col_name)
    return return_col


def _clean_album(df: DataFrame, col_name="album"):
    c = lower(trim(col(col_name)))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    cleaned = regexp_replace(c, regex_str, "")
    return_col = cleaned.alias(col_name)
    return return_col


def _clean_artists(df: DataFrame, col_name="artists"):
    c = col(col_name)
    split_array = split(c, ",", -1)
    clean_element = lambda x: regexp_replace(trim(x), r"^\['|'\]$|^'|'$|^\[|\]$", "")
    transformed_array = transform(split_array, clean_element)
    moar_clean_element = lambda x: regexp_replace(
        trim(x), r"^\s*$|^\s*nan\s*$|^\s*null\s*$", ""
    )
    moar_transformed_array = transform(transformed_array, moar_clean_element)
    return_col = moar_transformed_array.alias(col_name)
    return return_col


def _clean_artist_ids(df: DataFrame, col_name="artist_ids"):
    c = col(col_name)
    split_array = split(c, ",", -1)
    clean_element = lambda x: regexp_replace(trim(x), r"^\['|'\]$|^'|'$|^\[|\]$", "")
    transformed_array = transform(split_array, clean_element)
    moar_clean_element = lambda x: regexp_replace(
        trim(x), r"^\s*$|^\s*nan\s*$|^\s*null\s*$", ""
    )
    moar_transformed_array = transform(transformed_array, moar_clean_element)
    return_col = moar_transformed_array.alias(col_name)
    return return_col


def _clean_explicit(df: DataFrame, col_name="explicit"):
    c = col(col_name)
    lowered = lower(trim(c))
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    cleaned1 = regexp_replace(lowered, regex_str, "")
    cleaned2 = regexp_replace(cleaned1, "true", lit(True))
    cleaned3 = regexp_replace(cleaned2, "false", lit(False))
    return_col = cleaned3.alias(col_name)
    return return_col


def _clean_danceability(df: DataFrame, col_name="danceability"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_energy(df: DataFrame, col_name="energy"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_loudness(df: DataFrame, col_name="loudness"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_mode(df: DataFrame, col_name="mode"):
    c = col(col_name).cast(StringType())
    regex_str = r"^$|^\s*$|^\s*nan\s*$|^\s*null\s*$"
    cleaned1 = regexp_replace(c, regex_str, "")
    cleaned2 = regexp_replace(cleaned1, "0", "Minor")
    cleaned3 = regexp_replace(cleaned2, "1", "Major")
    return_col = cleaned3.alias(col_name)
    return return_col


def _clean_speechiness(df: DataFrame, col_name="speechiness"):
    c = col(col_name)
    rounded = round(c, 4)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_acousticness(df: DataFrame, col_name="acousticness"):
    c = col(col_name)
    rounded = round(c, 4)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_instrumentalness(df: DataFrame, col_name="instrumentalness"):
    c = col(col_name)
    rounded = round(c, 4)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_liveness(df: DataFrame, col_name="liveness"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_valence(df: DataFrame, col_name="valence"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_tempo(df: DataFrame, col_name="tempo"):
    c = col(col_name)
    rounded = round(c, 3)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_duration_ms(df: DataFrame, col_name="duration_ms"):
    c = expr(f"{col_name} / 1000")
    rounded = round(c, 0)
    return_col = rounded.alias(col_name)
    return return_col


def _clean_release_date(df: DataFrame, col_name="release_date"):
    date_format = "yyyy-MM-dd"
    c = col(col_name)
    date = to_date(c, date_format)
    return_col = date.alias(col_name)
    return return_col


def _check_num_nulls(df: DataFrame, col_list: str):
    expr_str = ""
    for col_name in col_list:
        if expr_str:
            expr_str += " OR "
        expr_str += f"{col_name} IS NULL"

    num_nulls = df.filter(expr(expr_str)).count()

    num_rows = df.count()

    if num_rows == 0:
        return 0.0

    rate_nulls = num_nulls / num_rows
    return rate_nulls, num_nulls, num_rows


def _check_duplicates(df: DataFrame, col_list: list):
    pass
