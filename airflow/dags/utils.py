from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def move_minio_files(
    source_prefix: str,
    dest_prefix: str,
    minio_conn_id="s3_minio",
    bucket_name="spark-data",
) -> None:
    s3_hook = S3Hook(aws_conn_id=minio_conn_id)
    key_list = s3_hook.list_keys(
        bucket_name=bucket_name,
        prefix=source_prefix,
    )
    if not key_list:
        return

    copied_keys = []

    for key in key_list:
        if key == source_prefix:
            continue

        try:
            s3_hook.copy_object(
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name,
                source_bucket_key=key,
                dest_bucket_key=key.replace(source_prefix, dest_prefix),
            )
        except Exception as e:
            print(f"Failed to copy {key} to {key.replace(source_prefix, dest_prefix)}")
            print(e)
            continue
        else:
            copied_keys.append(key)

    s3_hook.delete_objects(
        bucket=bucket_name,
        keys=copied_keys,
    )

    print(f"Moved files from {source_prefix} to {dest_prefix} in bucket {bucket_name}.")


def _create_gold_table():
    sql = """
    CREATE TABLE IF NOT EXISTS gold_table (
    id VARCHAR(255) PRIMARY KEY,
    name TEXT,
    album TEXT,
    album_id VARCHAR(255),
    artists VARCHAR(255) ARRAY,
    artist_ids VARCHAR(255) ARRAY,
    track_number INTEGER,
    disc_number INTEGER,
    explicit BOOLEAN,
    danceability DOUBLE PRECISION,
    energy DOUBLE PRECISION,
    key INTEGER,
    loudness DOUBLE PRECISION,
    mode VARCHAR(5),
    speechiness DOUBLE PRECISION,
    acousticness DOUBLE PRECISION,
    instrumentalness DOUBLE PRECISION,
    liveness DOUBLE PRECISION,
    valence DOUBLE PRECISION,
    tempo DOUBLE PRECISION,
    duration_ms BIGINT,
    time_signature INTEGER,
    year INTEGER,
    release_date DATE                   
    );
    """

    return sql
