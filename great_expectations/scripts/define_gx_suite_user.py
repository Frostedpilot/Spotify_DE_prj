import great_expectations as gx
from great_expectations.exceptions import DataContextError
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import datetime
import logging
import sys

# --- Configuration ---
# Path to your Great Expectations project directory
CONTEXT_ROOT_DIR = "/opt/airflow/great_expectations"  # Adjust if running from a different relative location

# Name of the Expectation Suite to create/update
EXPECTATION_SUITE_NAME = "silver_gx_user"  # Use a distinct name initially if needed

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)


def define_gx_suite_manually():
    log.info("Starting Manual Great Expectations Suite Definition.")
    log.info(f"Attempting to load GX context from: {CONTEXT_ROOT_DIR}")
    try:
        context = gx.get_context(context_root_dir=CONTEXT_ROOT_DIR)
    except DataContextError as e:
        log.error(f"Fatal: Error getting Great Expectations context: {e}")
        sys.exit(1)

    log.info(f"Constructing Expectation Suite: {EXPECTATION_SUITE_NAME}")

    suite = ExpectationSuite(expectation_suite_name=EXPECTATION_SUITE_NAME)

    suite.meta = {
        "great_expectations_version": gx.__version__,
        "citations": [
            {
                "citation_date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "comment": f"Suite '{EXPECTATION_SUITE_NAME}' created/updated manually by script 'define_gx_suite_silver.py'.",
            }
        ],
    }

    log.info("Adding expectations to the suite...")
    try:
        all_columns = [
            "ts",
            "id",
            "platform",
            "seconds_played",
            "track_name",
            "album_artist_name",
            "album_name",
            "type",
        ]

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={"column_list": all_columns},
            )
        )

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_select_column_values_to_be_unique_within_record",
                kwargs={"column_list": all_columns},
            )
        )

        not_null_columns = [
            "ts",
            "id",
            "seconds_played",
            "track_name",
            "album_artist_name",
        ]

        for column in not_null_columns:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": column},
                )
            )

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "seconds_played",
                    "min_value": 0,
                },
            )
        )

    except Exception as e:
        log.error(f"Fatal: Error occurred while adding expectations: {e}")
        sys.exit(1)  # Exit if definition fails

    log.info("Saving Expectation Suite...")
    try:
        # Save the constructed suite object (overwrites if suite with same name exists)
        context.save_expectation_suite(
            expectation_suite=suite, expectation_suite_name=EXPECTATION_SUITE_NAME
        )
        log.info(
            f"Expectation Suite '{EXPECTATION_SUITE_NAME}' saved successfully to {CONTEXT_ROOT_DIR}/expectations!"
        )
    except Exception as e:
        log.error(f"Fatal: Error saving Expectation Suite: {e}")
        sys.exit(1)  # Exit if saving fails

    log.info("Script finished successfully.")


if __name__ == "__main__":
    define_gx_suite_manually()
