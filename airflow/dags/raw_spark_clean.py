from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

import pendulum

from extract_minio import test_connection

# ---config---#
MINIO_CONN_ID = "s3_minio"
MINIO_BUCKET = "spark-data"
MINIO_INPUT = "raw_uploaded_csvs/latest/"

# ---DAG---#
with DAG(
    dag_id="raw_spark_clean",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "clean", "etl"],
    default_args={
        "owner": "airflow",
        "retries": 1,
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

    spark_clean_task = SparkSubmitOperator(
        task_id="spark_clean",
        application="/opt/bitnami/spark/apps/raw_spark_clean.py",
        name="raw_spark_clean",
        conn_id="spark_default",
        application_args=[
            "--file_path",
            f"s3a://{MINIO_BUCKET}/{MINIO_INPUT}",
        ],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        verbose=True,
    )

    test_connection_task >> spark_clean_task
