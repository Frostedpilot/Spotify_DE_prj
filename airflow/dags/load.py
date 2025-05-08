from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

import pendulum

from utils import move_minio_files, _create_gold_table

# ---config---#
MINIO_CONN_ID = "s3_minio"
AIRFLOW_POSTGRES_CONN_ID = "Gold_postgres"

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
    dag_id="load",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "load", "etl"],
    default_args={
        "owner": "airflow",
    },
) as dag:

    start_here = EmptyOperator(
        task_id="start_here",
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

    # move_gold_task = PythonOperator(
    #     task_id="move_gold",
    #     python_callable=move_minio_files,
    #     op_kwargs={
    #         "source_prefix": MINIO_GOLD,
    #         "dest_prefix": MINIO_GOLD.replace("latest", "archive"),
    #         "minio_conn_id": MINIO_CONN_ID,
    #         "bucket_name": MINIO_BUCKET,
    #     },
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    load_task = SparkSubmitOperator(
        task_id="load",
        application="/opt/bitnami/spark/apps/load.py",
        name="load",
        conn_id="spark_default",
        application_args=[
            "--file_path",
            f"s3a://{MINIO_BUCKET}/{MINIO_GOLD}",
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

    start_here >> test_postgres_task >> create_table_task >> load_task
