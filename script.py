import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import json
import time
import os
import math

# --- Configuration ---
INPUT_CSV_PATH = (
    "airflow\original\chunked\split_005.csv"  # Path to your CSV file with track IDs
)
OUTPUT_CSV_PATH = (
    "airflow\original\chunked\split_005_AHHH.csv"  # Path to save the results
)
ID_COLUMN_NAME = "id"  # Column name containing track IDs in your input CSV
BATCH_SIZE = 50  # Spotify API limit for fetching multiple tracks
RETRY_ATTEMPTS = 5  # Max retries for rate limiting errors
INITIAL_RETRY_DELAY = 5  # Initial delay (seconds) for retries

# --- Load Environment Variables ---
with open("secret.json") as f:
    secrets = json.load(f)
client_id = secrets.get("SPOTIPY_CLIENT_ID")
client_secret = secrets.get("SPOTIPY_CLIENT_SECRET")

if not client_id or not client_secret:
    raise ValueError(
        "SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET must be set in .env file or environment variables."
    )

# --- Authenticate with Spotify ---
try:
    client_credentials_manager = SpotifyClientCredentials(
        client_id=client_id, client_secret=client_secret
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    print("Successfully authenticated with Spotify.")
except Exception as e:
    print(f"Error during authentication: {e}")
    exit()

# --- Load Track IDs ---
try:
    print(f"Loading track IDs from {INPUT_CSV_PATH}...")
    df_ids = pd.read_csv(INPUT_CSV_PATH)
    if ID_COLUMN_NAME not in df_ids.columns:
        raise ValueError(f"Column '{ID_COLUMN_NAME}' not found in {INPUT_CSV_PATH}")

    # Drop rows with missing IDs and get unique IDs
    track_ids = df_ids[ID_COLUMN_NAME].dropna().unique().tolist()
    total_ids = len(track_ids)
    print(f"Found {total_ids} unique track IDs.")

except FileNotFoundError:
    print(f"Error: Input file not found at {INPUT_CSV_PATH}")
    exit()
except Exception as e:
    print(f"Error reading CSV file: {e}")
    exit()

# --- Fetch Popularity in Batches ---
all_results = []
processed_count = 0

num_batches = math.ceil(total_ids / BATCH_SIZE)
print(f"Starting popularity fetch in {num_batches} batches...")

for i in range(0, total_ids, BATCH_SIZE):
    batch_ids = track_ids[i : i + BATCH_SIZE]
    batch_num = (i // BATCH_SIZE) + 1
    print(f"Processing Batch {batch_num}/{num_batches} ({len(batch_ids)} IDs)...")

    attempts = 0
    while attempts < RETRY_ATTEMPTS:
        try:
            # Make the API call
            results = sp.tracks(batch_ids)

            # Process successful results
            for track in results["tracks"]:
                if (
                    track
                ):  # Check if track data was returned (might be None for invalid IDs)
                    all_results.append(
                        {"id": track["id"], "popularity": track["popularity"]}
                    )
                # else:
                #     print(f"  - Warning: No data returned for an ID in this batch (potentially invalid ID).")

            processed_count += len(batch_ids)
            print(f"  -> Success. Processed {processed_count}/{total_ids} total IDs.")

            # Short delay to be polite to the API even on success
            time.sleep(0.1)
            break  # Exit retry loop on success

        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:  # Rate limiting
                retry_after = int(
                    e.headers.get("Retry-After", INITIAL_RETRY_DELAY * (2**attempts))
                )  # Use header if available, else exponential backoff
                print(
                    f"  -> Rate limit exceeded. Retrying attempt {attempts + 1}/{RETRY_ATTEMPTS} after {retry_after} seconds..."
                )
                time.sleep(retry_after)
                attempts += 1
            else:
                print(
                    f"  -> Spotify API error (Status {e.http_status}): {e.msg}. Skipping batch."
                )
                # Decide if you want to break or continue with next batch on other errors
                break
        except Exception as e:
            print(
                f"  -> An unexpected error occurred: {e}. Retrying attempt {attempts + 1}/{RETRY_ATTEMPTS}..."
            )
            time.sleep(INITIAL_RETRY_DELAY * (2**attempts))
            attempts += 1

    if attempts == RETRY_ATTEMPTS:
        print(
            f"  -> Failed to process Batch {batch_num} after {RETRY_ATTEMPTS} attempts. Skipping."
        )
        # Log skipped batch_ids if necessary
        # with open('failed_batches.log', 'a') as f:
        #     f.write(f"Batch {batch_num}: {','.join(batch_ids)}\n")


# --- Save Results ---
if all_results:
    print(f"\nSaving {len(all_results)} results to {OUTPUT_CSV_PATH}...")
    df_results = pd.DataFrame(all_results)
    df_results.to_csv(OUTPUT_CSV_PATH, index=False)
    print("Done.")
else:
    print("\nNo popularity data was successfully fetched.")
