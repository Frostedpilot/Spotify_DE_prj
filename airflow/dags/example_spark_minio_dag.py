from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_minio_example",
    schedule=None,  # Or set a schedule e.g., "@daily"
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "minio", "example"],
) as dag:
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_simple_spark_job",
        application="/opt/bitnami/spark/apps/simple_spark_job.py",  # Path INSIDE the Spark containers
        conn_id="spark_default",  # Matches the connection ID set up in Airflow UI
        application_args=[
            "--input_path",
            "s3a://spark-data/input/my_data.csv",  # Input path in MinIO
            "--output_path",
            "s3a://spark-data/output/processed_data",  # Output path in MinIO
        ],
        # If you didn't configure S3A globally (spark-defaults or connection extra),
        # you might need to add conf here, but it's better practice to configure globally.
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        packages="org.apache.hadoop:hadoop-aws:3.3.4",  # Ensure S3A dependency is available
        # Adjust version if needed based on Spark/Hadoop
        # Often bundled in bitnami/spark, but explicit is safer
        verbose=True,  # Log spark-submit command and output
    )
