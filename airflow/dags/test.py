import logging
import os
import pendulum  # Using pendulum as in previous examples

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException

# Path to your Great Expectations project root inside the Airflow container
GX_PROJECT_ROOT_DIR = "/opt/airflow/great_expectations"
# (Optional) Name of a simple checkpoint you might have for a more thorough test
# If you don't have one, set this to None or an empty string
OPTIONAL_SIMPLE_CHECKPOINT_NAME = None  # Or 'my_very_simple_test_checkpoint'


# --- Python Callables for the Tasks ---
def initialize_gx_context_callable(**context):  # Added **context for ti if needed
    """
    Tests basic Great Expectations context loading.
    """
    log = logging.getLogger(__name__)
    log.info("Attempting to import Great Expectations...")
    log.info(f"MINIO_ENDPOINT_URL: {os.environ.get('MINIO_ENDPOINT_URL')}")
    log.info(f"AWS_ACCESS_KEY_ID (from env): {os.environ.get('MINIO_ACCESS_KEY')}")
    log.info(f"AWS_SECRET_ACCESS_KEY (from env): {os.environ.get('MINIO_SECRET_KEY')}")
    import great_expectations as gx

    try:

        log.info(f"Successfully imported Great Expectations version: {gx.__version__}")
    except ImportError as e:
        log.error(f"Failed to import Great Expectations: {e}")
        raise AirflowException(
            "Great Expectations library not found in Airflow environment."
        )

    log.info(f"Attempting to load GX Data Context from: {GX_PROJECT_ROOT_DIR}")
    if not os.path.isdir(GX_PROJECT_ROOT_DIR):
        log.error(
            f"GX project directory not found at {GX_PROJECT_ROOT_DIR}. Check volume mounts."
        )
        raise AirflowException(f"GX project directory not found: {GX_PROJECT_ROOT_DIR}")
    if not os.path.exists(os.path.join(GX_PROJECT_ROOT_DIR, "great_expectations.yml")):
        log.error(
            f"great_expectations.yml not found in {GX_PROJECT_ROOT_DIR}. Is this a valid GX project root?"
        )
        raise AirflowException("great_expectations.yml not found in project root.")

    try:
        # Explicitly provide context_root_dir to ensure it finds the project
        gx_context = gx.get_context(context_root_dir=GX_PROJECT_ROOT_DIR)
        log.info(
            f"Successfully loaded GX Data Context. Project name: {gx_context.project_config.project_name}"
        )
        log.info(f"Available datasources: {list(gx_context.datasources.keys())}")
        log.info(
            f"Available expectation suites: {gx_context.list_expectation_suite_names()}"
        )
        log.info(f"Available checkpoints: {gx_context.list_checkpoints()}")
        return "GX context loaded successfully."  # XCom pushed automatically
    except Exception as e:
        log.error(f"Failed to load GX Data Context: {e}", exc_info=True)
        raise AirflowException(
            f"Failed to load GX Data Context from {GX_PROJECT_ROOT_DIR}."
        )


def optionally_run_simple_checkpoint_callable(checkpoint_name: str | None, **context):
    """
    (Optional) Tries to run a very simple checkpoint if provided.
    The 'gx_init_status' XCom is implicitly available if needed but not used here.
    """
    log = logging.getLogger(__name__)
    # ti = context['ti'] # Get task instance if you need to pull XComs explicitly
    # gx_init_status = ti.xcom_pull(task_ids='initialize_gx_context_task') # Example XCom pull

    if not checkpoint_name:
        log.info("No checkpoint name provided, skipping checkpoint run test.")
        return "Checkpoint run skipped."

    log.info(f"Attempting to run GX checkpoint: {checkpoint_name}")
    try:
        import great_expectations as gx

        gx_context = gx.get_context(context_root_dir=GX_PROJECT_ROOT_DIR)
        result = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)
        log.info(
            f"Checkpoint '{checkpoint_name}' run completed. Success: {result['success']}"
        )
        if not result["success"]:
            log.warning(
                f"Checkpoint '{checkpoint_name}' did not pass. This might be okay for a simple test if data isn't present."
            )
            log.warning(
                f"Validation result details: {result.list_validation_results()}"
            )
        return f"Checkpoint '{checkpoint_name}' run. Success: {result['success']}"
    except Exception as e:
        log.error(
            f"Failed to run GX checkpoint '{checkpoint_name}': {e}", exc_info=True
        )
        return f"Error running checkpoint '{checkpoint_name}': {e}"


# --- DAG Definition ---
with DAG(
    dag_id="test_great_expectations_installation_traditional",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),  # Using pendulum
    catchup=False,
    tags=["gx", "test"],
    default_args={
        "owner": "airflow",
        # 'retries': 1, # Add other default args if you use them
    },
) as dag:

    initialize_gx_context_task = PythonOperator(
        task_id="initialize_gx_context_task",
        python_callable=initialize_gx_context_callable,
        # provide_context=True, # Not strictly needed if only using implicit XCom return
    )

    optionally_run_simple_checkpoint_task = PythonOperator(
        task_id="optionally_run_simple_checkpoint_task",
        python_callable=optionally_run_simple_checkpoint_callable,
        op_kwargs={
            "checkpoint_name": OPTIONAL_SIMPLE_CHECKPOINT_NAME,
        },
        # provide_context=True, # Needed if you want to explicitly pull XComs within the callable
    )

    # Define task dependencies
    initialize_gx_context_task >> optionally_run_simple_checkpoint_task
