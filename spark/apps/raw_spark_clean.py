from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, lit
import argparse
from datetime import datetime
import logging
import sys
from cleaning_utils import (
    load_data_to_spark,
    _clean_name,
    _clean_album,
    _clean_artists,
    _clean_artist_ids,
    _clean_explicit,
    _clean_danceability,
    _clean_energy,
    _clean_key,
    _clean_loudness,
    _clean_mode,
    _clean_speechiness,
    _clean_acousticness,
    _clean_instrumentalness,
    _clean_liveness,
    _clean_valence,
    _clean_tempo,
    _clean_duration_ms,
    _clean_release_date,
)


def spark_clean(logger, df: DataFrame):
    # Start the cleaning process
    logger.info("Starting data cleaning process")
    logger.info(f"DataFrame schema before cleaning: {df.printSchema()}")
    logger.info(f"DataFrame count before cleaning: {df.count()}")
    logger.info(f"DataFrame columns before cleaning: {df.columns}")

    logger.info("No need to clean column id")
    i = col("id")

    logger.info("Start cleaning column name")
    name = None
    try:
        name = _clean_name(df, col_name="name")
        logger.info("Column name cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column name: {e}")
        raise e

    logger.info("Start cleaning column album")
    album = None
    try:
        album = _clean_album(df, col_name="album")
        logger.info("Column album cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column album: {e}")
        raise e

    logger.info("No need to clean column album_id")
    album_id = col("album_id")

    logger.info("Start cleaning column artists")
    artists = None
    try:
        artists = _clean_artists(df, col_name="artists")
        logger.info("Column artists cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column artists: {e}")
        raise e

    logger.info("Start cleaning column artist_id")
    artist_id = None
    try:
        artist_id = _clean_artist_ids(df, col_name="artist_ids")
        logger.info("Column artist_id cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column artist_id: {e}")
        raise e

    logger.info("No need to clean column track_number")
    track_number = col("track_number")

    logger.info("No need to clean column disc_number")
    disc_number = col("disc_number")

    logger.info("Start cleaning column explicit")
    explicit = None
    try:
        explicit = _clean_explicit(df, col_name="explicit")
        logger.info("Column explicit cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column explicit: {e}")
        raise e

    logger.info("Start cleaning column danceability")
    danceability = None
    try:
        danceability = _clean_danceability(df, col_name="danceability")
        logger.info("Column danceability cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column danceability: {e}")
        raise e

    logger.info("Start cleaning column energy")
    energy = None
    try:
        energy = _clean_energy(df, col_name="energy")
        logger.info("Column energy cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column energy: {e}")
        raise e

    logger.info("Start cleaning column key")
    key = None
    try:
        key = _clean_key(df, col_name="key")
        logger.info("Column key cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column key: {e}")
        raise e

    logger.info("Start cleaning column loudness")
    loudness = None
    try:
        loudness = _clean_loudness(df, col_name="loudness")
        logger.info("Column loudness cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column loudness: {e}")
        raise e

    logger.info("Start cleaning column mode")
    mode = None
    try:
        mode = _clean_mode(df, col_name="mode")
        logger.info("Column mode cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column mode: {e}")
        raise e

    logger.info("Start cleaning column speechiness")
    speechiness = None
    try:
        speechiness = _clean_speechiness(df, col_name="speechiness")
        logger.info("Column speechiness cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column speechiness: {e}")
        raise e

    logger.info("Start cleaning column acousticness")
    acousticness = None
    try:
        acousticness = _clean_acousticness(df, col_name="acousticness")
        logger.info("Column acousticness cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column acousticness: {e}")
        raise e

    logger.info("Start cleaning column instrumentalness")
    instrumentalness = None
    try:
        instrumentalness = _clean_instrumentalness(df, col_name="instrumentalness")
        logger.info("Column instrumentalness cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column instrumentalness: {e}")
        raise e

    logger.info("Start cleaning column liveness")
    liveness = None
    try:
        liveness = _clean_liveness(df, col_name="liveness")
        logger.info("Column liveness cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column liveness: {e}")
        raise e

    logger.info("Start cleaning column valence")
    valence = None
    try:
        valence = _clean_valence(df, col_name="valence")
        logger.info("Column valence cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column valence: {e}")
        raise e

    logger.info("Start cleaning column tempo")
    tempo = None
    try:
        tempo = _clean_tempo(df, col_name="tempo")
        logger.info("Column tempo cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column tempo: {e}")
        raise e

    logger.info("Start cleaning column duration_ms")
    duration_ms = None
    try:
        duration_ms = _clean_duration_ms(df, col_name="duration_ms")
        logger.info("Column duration_ms cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column duration_ms: {e}")
        raise e

    logger.info("No need to clean column time_signature")
    time_signature = col("time_signature")

    logger.info("No need to clean column year")
    year = col("year")

    logger.info("Start cleaning column release_date")
    release_date = None
    try:
        release_date = _clean_release_date(df, col_name="release_date")
        logger.info("Column release_date cleaned successfully")
    except Exception as e:
        logger.error(f"Error cleaning column release_date: {e}")
        raise e

    # logger.info("Start cleaning column popularity")
    # popularity = None
    # try:
    #     popularity = col("popularity")
    # except Exception as e:
    #     logger.error(f"Error cleaning column popularity: {e}")
    #     raise e

    return [
        i,
        name,
        album,
        album_id,
        artists,
        artist_id,
        track_number,
        disc_number,
        explicit,
        danceability,
        energy,
        key,
        loudness,
        mode,
        speechiness,
        acousticness,
        instrumentalness,
        liveness,
        valence,
        tempo,
        duration_ms,
        time_signature,
        year,
        release_date,
        # popularity,
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("spark_clean.log"),
        ],
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting Spark Raw Cleaning Pipeline - {current_time}")
    spark = SparkSession.builder.appName("Spark Raw Cleaning Pipeline").getOrCreate()
    s3_path = "s3a://spark-data/raw_uploaded_csvs/song/latest/"
    s3_log_path = "s3a://spark-data/logs/song/"
    file_path = args.file_path
    logger.info(f"File path: {file_path}")

    # Load data to Spark DataFrame
    try:
        df = load_data_to_spark(file_path, spark, data_type="raw")
        print(df.show(5))
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise e

    col_lst = spark_clean(logger, df)
    col_lst = [i if i is not None else lit(None) for i in col_lst]
    logger.info("Data cleaning process completed successfully")

    df_cleaned = df.select(*col_lst)

    print(df_cleaned.show(5))

    logger.info(f"DataFrame schema after cleaning: {df_cleaned.printSchema()}")
    logger.info(f"DataFrame count after cleaning: {df_cleaned.count()}")
    logger.info(f"DataFrame columns after cleaning: {df_cleaned.columns}")

    # Save cleaned DataFrame to S3
    s3_output_path = "s3a://spark-data/silver_data/song/latest/"
    logger.info(f"Saving cleaned DataFrame parquet to {s3_output_path}")
    try:
        df_cleaned.write.mode("overwrite").parquet(s3_output_path)
        logger.info("Data saved successfully")
    except Exception as e:
        logger.error(f"Error saving data: {e}")
        raise e
    finally:
        spark.stop()
        logger.info("Spark session stopped")
        logger.info("Pipeline completed")
        logger.info(f"End of Spark Raw Cleaning Pipeline - {current_time}")
