import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.exceptions import DataContextError
import datetime
import logging
import sys

# --- Configuration ---
# Path to your Great Expectations project directory
CONTEXT_ROOT_DIR = (
    "/opt/airflow/gx"  # Adjust if running from a different relative location
)

# Name of the Expectation Suite to create/update
EXPECTATION_SUITE_NAME = "user_gx"  # Use a distinct name initially if needed

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

    suite = ExpectationSuite(name=EXPECTATION_SUITE_NAME)

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
        es = []
        log.info("Adding all_columns expectation...")
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

        es.append(
            gx.expectations.ExpectTableColumnsToMatchOrderedList(
                column_list=all_columns,
                result_format="COMPLETE",
            )
        )

        es.append(
            gx.expectations.ExpectSelectColumnValuesToBeUniqueWithinRecord(
                column_list=all_columns,
                result_format="COMPLETE",
            )
        )

        log.info("Adding column null expectations...")

        not_null_columns = [
            "ts",
            "id",
            "seconds_played",
            "track_name",
            "album_artist_name",
        ]

        for column in not_null_columns:
            es.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=column,
                    result_format="COMPLETE",
                )
            )

        log.info("Adding column value expectations...")

        es.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="seconds_played",
                min_value=0,
                result_format="COMPLETE",
            )
        )

    except Exception as e:
        log.error(f"Fatal: Error occurred while adding expectations: {e}")
        sys.exit(1)  # Exit if definition fails

    log.info("Saving Expectation Suite...")
    try:
        # Save the constructed suite object (overwrites if suite with same name exists)

        for expectation in es:
            suite.add_expectation(expectation)

        context.suites.add_or_update(suite)

        log.info(
            f"Expectation Suite '{EXPECTATION_SUITE_NAME}' saved successfully to {CONTEXT_ROOT_DIR}/expectations!"
        )
    except Exception as e:
        log.error(f"Fatal: Error saving Expectation Suite: {e}")
        sys.exit(1)  # Exit if saving fails

    log.info("Script finished successfully.")


if __name__ == "__main__":
    define_gx_suite_manually()
