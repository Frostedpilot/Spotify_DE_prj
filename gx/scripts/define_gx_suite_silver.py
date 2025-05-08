import great_expectations as gx
from great_expectations.core import ExpectationSuite  # Import ExpectationSuite
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)  # Corrected import
from great_expectations.exceptions import DataContextError  # Import DataContextError
import datetime
import logging
import sys

# --- Configuration ---
# Path to your Great Expectations project directory
CONTEXT_ROOT_DIR = (
    "/opt/airflow/gx"  # Adjust if running from a different relative location
)

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

        es.append(
            gx.expectations.ExpectTableColumnsToMatchOrderedList(
                column_list=all_columns,
                result_format="COMPLETE",
            )
        )

        log.info("Adding column null expectations...")

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
            es.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column=col, result_format="COMPLETE"
                )
            )

        log.info("Adding column type expectations: int...")

        int_columns = [
            "track_number",
            "disc_number",
            "duration_ms",
            "time_signature",
            "year",
        ]
        for col in int_columns:
            es.append(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=col, type_="int", result_format="COMPLETE"
                )
            )

        log.info("Adding column type expectations: float...")

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
            es.append(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=col, type_="float", result_format="COMPLETE"
                )
            )

        log.info("Adding column range expectaions...")

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
            es.append(
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column=col,
                    min_value=0.0,
                    max_value=1.0,
                    result_format="COMPLETE",
                )
            )

        log.info("Adding column type expectations:bool...")

        es.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column="explicit", type_="bool", result_format="COMPLETE"
            )
        )

        log.info("Adding column set expectations...")

        es.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="mode",
                value_set=["Major", "Minor"],
                result_format="COMPLETE",
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

        es.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="key",
                value_set=key_codes,
                result_format="COMPLETE",
            )
        )

        log.info("Adding column year expectations...")

        this_year = datetime.datetime.now().year

        es.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1800,
                max_value=this_year,
                result_format="COMPLETE",
            )
        )

        log.info("Adding column unique expectations...")

        es.append(
            gx.expectations.ExpectColumnValuesToBeUnique(
                column="id", result_format="COMPLETE"
            )
        )

    except Exception as e:
        log.error(f"Fatal: Error occurred while adding expectations: {e}")
        sys.exit(1)  # Exit if definition fails

    log.info("Saving Expectation Suite...")
    try:

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
