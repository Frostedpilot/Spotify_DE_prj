from __future__ import annotations

import os
import logging
import pendulum
import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

# ---config---#
LOCAL_INPUT_DIR = "/opt/airflow/input/user_data/"
MINIO_CONN_ID = "s3_minio"
MINIO_PREFIX = "raw_uploaded_csvs/user/latest/"

MINIO_BUCKET = Variable.get("MINIO_BUCKET")


# ---tasks functions---#
def upload_to_s3(
    local_dir=LOCAL_INPUT_DIR,
    bucket_name=MINIO_BUCKET,
    prefix=MINIO_PREFIX,
    conn_id=MINIO_CONN_ID,
):
    log = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log.info(f"From airflow DAG: {__name__} - {current_time} - task_id: upload_to_s3")
    log.info(f"Scanning directory: {local_dir}")

    if not os.path.exists(local_dir):
        log.error(f"Local directory {local_dir} does not exist.")
        raise FileNotFoundError(f"Local directory {local_dir} does not exist.")

    files_uploaded = 0
    s3_hook = S3Hook(aws_conn_id=conn_id)
    for file_name in os.listdir(local_dir):
        if file_name.endswith(".csv"):
            print(f"Found CSV file: {file_name}")
            local_file_path = os.path.join(local_dir, file_name)
            s3_path = os.path.join(prefix, file_name)
            try:
                df = pd.read_csv(local_file_path)
                # Drop sensitive fields
                df = df.drop(columns=["ip_addr"], inplace=False, errors="ignore")
                df.to_parquet(local_file_path.replace(".csv", ".parquet"), index=False)
                print(f"Converted {local_file_path} to Parquet format.")
                local_file_path = local_file_path.replace(".csv", ".parquet")
                s3_path = s3_path.replace(".csv", ".parquet")
                log.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_path}")
                s3_hook.load_file(
                    filename=local_file_path,
                    bucket_name=bucket_name,
                    key=s3_path,
                    replace=True,
                )
                files_uploaded += 1
            except Exception as e:
                log.error(
                    f"Failed to upload {local_file_path} to s3://{bucket_name}/{s3_path}: {e}"
                )
                log.error(f"Error details: {e}")
                raise e

    if files_uploaded == 0:
        log.warning(f"No CSV files found in {local_dir}.")
    else:
        log.info(
            f"Successfully uploaded {files_uploaded} CSV files to s3://{bucket_name}/{prefix}."
        )


def test_connection(conn_id=MINIO_CONN_ID, bucket_name=MINIO_BUCKET):
    log = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log.info(
        f"From airflow DAG: {__name__} - {current_time} - task_id: test_connection"
    )
    s3_hook = S3Hook(aws_conn_id=conn_id)
    try:
        if s3_hook.check_for_bucket(bucket_name):
            log.info(f"Connection to MinIO bucket {bucket_name} is successful.")
            return True
        else:
            log.error(f"Bucket {bucket_name} does not exist in MinIO.")
            return False
    except Exception as e:
        log.error(f"Failed to connect to MinIO: {e}")
        return False


# ---DAG---#
with DAG(
    dag_id="load_minio_but_user",
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["extract", "etl"],
    default_args={
        "owner": "airflow",
    },
) as dag:
    test_connection_task = PythonOperator(
        task_id="test_connection",
        python_callable=test_connection,
        op_kwargs={
            "conn_id": MINIO_CONN_ID,
            "bucket_name": MINIO_BUCKET,
        },
    )

    upload_task = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_s3,
        op_kwargs={
            "local_dir": LOCAL_INPUT_DIR,
            "bucket_name": MINIO_BUCKET,
            "prefix": MINIO_PREFIX,
            "conn_id": MINIO_CONN_ID,
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id="trigger_dag",
        trigger_dag_id="raw_spark_clean_but_user",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    test_connection_task >> upload_task >> trigger_dag_task
