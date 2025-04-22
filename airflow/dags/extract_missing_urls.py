import pendulum
import json

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from utils_missing_urls import mine

# --- Config ---
DB_CONN_ID = "Gold_postgres"
SPARK_CONN_ID = "spark_default"
SPARK_SCRIPT_PATH = "/path/to/your/spark/scripts/process_missing_ids.py"


def _get_missing_ids_from_db(**context):
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = """
    SELECT DISTINCT b.id
    FROM gold_user_table b
    WHERE NOT EXISTS (
        SELECT 1
        FROM gold_table a
        WHERE a.id = b.id
    );
    """
    print(f"Executing query: {sql}")
    results = hook.get_records(sql=sql)
    print(f"Raw results count: {len(results)}")
    missing_ids = [row[0] for row in results]
    print(f"Found {len(missing_ids)} missing IDs.")

    if len(missing_ids) > 10000:
        print(
            "Warning: Large number of IDs found. Consider alternative methods if XCom limits are exceeded."
        )

    return missing_ids


def create_index():
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    sql = """
    CREATE INDEX idx_a_id ON gold_user_table (id);
    CREATE INDEX idx_b_id ON gold_table (id);
    """

    print(f"Creating indexes with query: {sql}")
    try:
        hook.run(sql=sql)
    except Exception as e:
        print(f"Error creating indexes: {e}")
    else:
        print("Indexes created successfully.")


# --- DAG Definition ---
with DAG(
    dag_id="find_and_process_missing_ids",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=["database", "spark"],
) as dag:

    client_id = Variable.get("Spotify_client_id")
    client_secret = Variable.get("Spotify_client_secret")

    task_create_index = PythonOperator(
        task_id="create_index",
        python_callable=create_index,
    )

    task_get_ids = PythonOperator(
        task_id="get_missing_ids",
        python_callable=_get_missing_ids_from_db,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    task_mine = PythonOperator(
        task_id="mine_missing_ids",
        python_callable=mine,
        op_kwargs={
            "id_list": "{{ task_instance.xcom_pull(task_ids='get_missing_ids') }}",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    next_task = TriggerDagRunOperator(
        task_id="trigger_next_dag",
        trigger_dag_id="load_minio",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    task_create_index >> task_get_ids >> task_mine >> next_task
