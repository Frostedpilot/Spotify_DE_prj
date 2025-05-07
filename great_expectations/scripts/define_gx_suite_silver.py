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
EXPECTATION_SUITE_NAME = "silver_gx_song"  # Use a distinct name initially if needed

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
            "id",
            "name",
            "album",
            "album_id",
            "artists",
            "artist_ids",
            "track_number",
            "disc_number",
            "explicit",
            "danceability",
            "energy",
            "key",
            "loudness",
            "mode",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
            "duration_ms",
            "time_signature",
            "year",
            "release_date",
        ]

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_table_columns_to_match_ordered_list",
                kwargs={"column_list": all_columns},
            )
        )

        # Only columns that are used later in view in Postgres and ids
        not_null_columns = [
            "id",
            "album_id",
            "artists",
            "artist_ids",
            "danceability",
            "energy",
            "key",
            "loudness",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
        ]

        for col in not_null_columns:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_not_be_null",
                    kwargs={"column": col},
                )
            )

        int_columns = [
            "track_number",
            "disc_number",
            "duration_ms",
            "time_signature",
            "year",
        ]
        for col in int_columns:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={
                        "column": col,
                        "type_": "int",
                    },
                )
            )

        float_columns = [
            "danceability",
            "energy",
            "loudness",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
            "tempo",
        ]

        for col in float_columns:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_of_type",
                    kwargs={
                        "column": col,
                        "type_": "float",
                    },
                )
            )

        cols_in_0_1 = [
            "danceability",
            "energy",
            "speechiness",
            "acousticness",
            "instrumentalness",
            "liveness",
            "valence",
        ]

        for col in cols_in_0_1:
            suite.add_expectation(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_between",
                    kwargs={
                        "column": col,
                        "min_value": 0.0,
                        "max_value": 1.0,
                    },
                )
            )

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={
                    "column": "explicit",
                    "type_": "bool",
                },
            )
        )

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "mode",
                    "value_set": ["Major", "Minor"],
                },
            )
        )

        key_codes = [
            "C",
            "C♯/D♭",
            "D",
            "D♯/E♭",
            "E",
            "F",
            "F♯/G♭",
            "G",
            "G♯/A♭",
            "A",
            "A♯/B♭",
            "B",
        ]

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={
                    "column": "key",
                    "value_set": key_codes,
                },
            )
        )

        this_year = datetime.datetime.now().year

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": "year",
                    "min_value": 1800,
                    "max_value": this_year,
                },
            )
        )

        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_unique",
                kwargs={"column": "id"},
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
