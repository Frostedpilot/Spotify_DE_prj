from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

import pendulum

from extract_minio import test_connection
from utils import move_minio_files, _create_gold_table

# ---config---#
MINIO_CONN_ID = "s3_minio"
MINIO_BUCKET = "spark-data"
MINIO_INPUT = "raw_uploaded_csvs/user/latest/"
MINIO_SILVER = "silver_data/user/latest/"
GOLD_DB = "gold_data_db"
AIRFLOW_POSTGRES_CONN_ID = "Gold_postgres"
DB_USER = "airflow"
DB_PASS = "airflow"

# ---DAG---#
with DAG(
    dag_id="raw_spark_clean_but_user",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "clean", "etl"],
    default_args={
        "owner": "airflow",
    },
) as dag:

    start_here = EmptyOperator(task_id="start_here")

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
        application="/opt/bitnami/spark/apps/raw_spark_clean_but_user.py",
        name="raw_spark_clean",
        conn_id="spark_default",
        application_args=[
            "--file_path",
            f"s3a://{MINIO_BUCKET}/{MINIO_INPUT}",
            "--gold_db",
            GOLD_DB,
            "--db_user",
            DB_USER,
            "--db_pass",
            DB_PASS,
        ],
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.5",
        py_files="/opt/bitnami/spark/apps/cleaning_utils.py",
        verbose=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    move_file_task = PythonOperator(
        task_id="move_file",
        python_callable=move_minio_files,
        op_kwargs={
            "source_prefix": MINIO_INPUT,
            "dest_prefix": MINIO_INPUT.replace("latest", "archive"),
            "minio_conn_id": MINIO_CONN_ID,
            "bucket_name": MINIO_BUCKET,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )
    test_postgres_task = PostgresOperator(
        task_id="test_postgres",
        postgres_conn_id=AIRFLOW_POSTGRES_CONN_ID,
        sql=f"""
        SELECT 1 FROM pg_database WHERE datname='{GOLD_DB}'
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=AIRFLOW_POSTGRES_CONN_ID,
        sql=_create_gold_table(),
        database=GOLD_DB,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    leaf_task = EmptyOperator(task_id="leaf_task", trigger_rule=TriggerRule.ALL_DONE)

    start_here >> test_connection_task >> spark_clean_task >> move_file_task
    start_here >> test_postgres_task >> create_table_task >> leaf_task
