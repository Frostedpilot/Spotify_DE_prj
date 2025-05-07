from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import pendulum

from extract_minio import test_connection
from utils import move_minio_files, _create_gold_table

# ---config---#
MINIO_CONN_ID = "s3_minio"
AIRFLOW_POSTGRES_CONN_ID = "Gold_postgres"

MINIO_INPUT = "raw_uploaded_csvs/song/latest/"
MINIO_SILVER = "silver_data/song/latest/"
MINIO_GOLD = "gold_data/song/latest/"

GOLD_DB = Variable.get("GOLD_DB")
MINIO_BUCKET = Variable.get("MINIO_BUCKET")
DB_USER = Variable.get("DB_USER")
DB_PASS = Variable.get("DB_PASSWORD")

spark_conf = {
    "spark.hadoop.fs.s3a.endpoint": Variable.get("MINIO_ENDPOINT"),
    "spark.hadoop.fs.s3a.access.key": Variable.get("MINIO_ACCESS_KEY"),
    "spark.hadoop.fs.s3a.secret.key": Variable.get("MINIO_SECRET_KEY"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
}


# ---DAG---#
with DAG(
    dag_id="raw_spark_clean",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "clean", "etl"],
    default_args={
        "owner": "airflow",
    },
) as dag:

    start_here = EmptyOperator(
        task_id="start_here",
    )

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
        conf=spark_conf,
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        py_files="/opt/bitnami/spark/apps/cleaning_utils.py",
        verbose=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    silver_clean_task = SparkSubmitOperator(
        task_id="silver_clean",
        application="/opt/bitnami/spark/apps/silver_spark_clean.py",
        name="silver_spark_clean",
        conn_id="spark_default",
        application_args=[
            "--file_path",
            f"s3a://{MINIO_BUCKET}/{MINIO_SILVER}",
            "--gold_db",
            GOLD_DB,
            "--db_user",
            DB_USER,
            "--db_pass",
            DB_PASS,
        ],
        conf=spark_conf,
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

    move_silver_task = PythonOperator(
        task_id="move_silver",
        python_callable=move_minio_files,
        op_kwargs={
            "source_prefix": MINIO_SILVER,
            "dest_prefix": MINIO_SILVER.replace("latest", "archive"),
            "minio_conn_id": MINIO_CONN_ID,
            "bucket_name": MINIO_BUCKET,
        },
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="load",
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        start_here
        >> test_connection_task
        >> spark_clean_task
        >> [move_file_task, silver_clean_task]
    )

    move_silver_task << silver_clean_task

    silver_clean_task >> trigger_load_dag
