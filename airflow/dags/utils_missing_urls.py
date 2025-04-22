import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import json
import time
import math
import random

# --- Configuration ---
BATCH_SIZE = 50
RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY = 5


def mine(id_list, client_id, client_secret):
    id_list = eval(id_list)  # Convert string representation of list to actual list
    if not isinstance(id_list, list):
        raise ValueError("id_list must be a list of Spotify track IDs.")

    print(id_list)
    print(f"Found {len(id_list)} IDs to process.")

    if not client_id or not client_secret:
        raise ValueError(
            "SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET must be set in .env file or environment variables."
        )

    try:
        client_credentials_manager = SpotifyClientCredentials(
            client_id=client_id, client_secret=client_secret
        )
        sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
        print("Successfully authenticated with Spotify.")
    except Exception as e:
        print(f"Error during authentication: {e}")
        exit()

    lst = []

    for i in range(len(id_list)):
        if id_list[i] is None:
            print(f"ID at index {i} is None. Skipping...")
            continue
        lst.append("spotify:track:" + id_list[i])

    print(f"Converted {len(id_list)} IDs to Spotify URLs.")

    all_results = []
    processed_count = 0
    total_ids = len(id_list)

    num_batches = math.ceil(total_ids / BATCH_SIZE)
    print(f"Starting popularity fetch in {num_batches} batches...")

    for i in range(0, total_ids, BATCH_SIZE):
        batch_ids = id_list[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        print(f"Processing Batch {batch_num}/{num_batches} ({len(batch_ids)} IDs)...")

        attempts = 0
        while attempts < RETRY_ATTEMPTS:
            try:
                # Make the API call
                results = sp.tracks(batch_ids)

                # Process successful results
                for track in results["tracks"]:
                    if track:
                        print(track)
                        all_results.append(
                            {
                                "id": track["id"],
                                "name": track["name"],
                                "album": track["album"]["name"],
                                "album_id": track["album"]["id"],
                                "artists": [
                                    artist["name"] for artist in track["artists"]
                                ],
                                "artist_id": [
                                    artist["id"] for artist in track["artists"]
                                ],
                                "track_number": track["track_number"],
                                "disc_number": track["disc_number"],
                                "explicit": track["explicit"],
                                "danceability": None,
                                "energy": None,
                                "key": None,
                                "loudness": None,
                                "mode": None,
                                "speechiness": None,
                                "acousticness": None,
                                "instrumentalness": None,
                                "liveness": None,
                                "valence": None,
                                "tempo": None,
                                "duration_ms": track["duration_ms"],
                                "time_signature": None,
                                "popularity": track["popularity"],
                                "release_date": track["album"]["release_date"],
                            }
                        )

                processed_count += len(batch_ids)
                print(
                    f"  -> Success. Processed {processed_count}/{total_ids} total IDs."
                )

                # Short delay to be polite to the API even on success
                sleep_time = random.uniform(0.1, 0.5)
                time.sleep(sleep_time)
                break  # Exit retry loop on success

            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:  # Rate limiting
                    retry_after = int(
                        e.headers.get(
                            "Retry-After", INITIAL_RETRY_DELAY * (2**attempts)
                        )
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

    # --- Upload to Minio ---
    df = pd.DataFrame(all_results)
    return df.to_parquet(
        "/opt/airflow/input/song_data/missing_urls.parquet",
        index=False,
        engine="pyarrow",
    )
