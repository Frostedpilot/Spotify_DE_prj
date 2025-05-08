from pyspark.sql import SparkSession, DataFrame
import argparse
from datetime import datetime
import great_expectations as gx
import logging
import sys
from great_expectations.core.batch import RuntimeBatchRequest
from cleaning_utils_but_user import (
    load_data_to_spark,
)

# -- config -- #
CONTEXT_DIR = "/opt/airflow/great_expectations"
DATASOURCE_NAME = "my_data_source"
DATA_ASSET_NAME = "my_dataframe_data_asset"


def validate_gx(df, suite):
    context = gx.get_context()
    data_source = context.data_sources.get(DATASOURCE_NAME)
    this_data_asset = data_source.add_dataframe_asset(
        name=DATA_ASSET_NAME, dataframe=df
    )
    batch_definition = this_data_asset.add_batch_definition_whole_dataframe(
        "my_batch_definition"
    )

    _suite = context.suites.get(suite)

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=_suite,
        name="my_validation_definition",
    )

    validation_result = validation_definition.run()

    print(validation_result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file_path", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("user_spark_clean.log"),
        ],
    )
    logger = logging.getLogger(__name__)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting GX Pipeline - {current_time}")
    spark = SparkSession.builder.appName("GX").getOrCreate()
    file_path = args.file_path
    logger.info(f"File path: {file_path}")

    # Load data to Spark DataFrame
    try:
        df = load_data_to_spark(file_path, spark)
        print(df.show(5))
        logger.info("Data loaded successfully")
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise e
