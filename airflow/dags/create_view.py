from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator

import pendulum

from utils import _create_view

# ---config---#
GOLD_DB = "gold_data_db"
AIRFLOW_POSTGRES_CONN_ID = "Gold_postgres"

# ---DAG---#
with DAG(
    dag_id="create_view",
    schedule=None,
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=["spark", "load"],
    default_args={
        "owner": "airflow",
    },
) as dag:

    start_here = EmptyOperator(task_id="start_here")

    create_view_task = PostgresOperator(
        task_id="create_view",
        postgres_conn_id=AIRFLOW_POSTGRES_CONN_ID,
        sql=_create_view(),
        database=GOLD_DB,
    )

    start_here >> create_view_task
