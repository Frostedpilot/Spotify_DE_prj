import json
import os
import pandas as pd

# --configs--

INPUT_JSON_FOLDER = "SpotifyStreams"
OUTPUT_CSV_PATH = "mystreams.csv"

df = None

file_list = os.listdir(INPUT_JSON_FOLDER)
print(f"Found {len(file_list)} files in {INPUT_JSON_FOLDER}.")
print(f"Files: {file_list}")

for file_name in file_list:
    if file_name.endswith(".json"):
        # --- Load JSON Data ---
        try:
            with open(INPUT_JSON_FOLDER + "/" + file_name, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"Successfully loaded JSON data from {INPUT_JSON_FOLDER}.")
        except FileNotFoundError:
            print(f"Error: Input file not found at {INPUT_JSON_FOLDER}")
            exit()

        # --- Convert JSON to DataFrame ---
        try:
            if df is None:
                df = pd.json_normalize(data)
            else:
                df_temp = pd.json_normalize(data)
                df = pd.concat([df, df_temp], ignore_index=True)
        except Exception as e:
            print(f"Error converting JSON to DataFrame: {e}")
            exit()

print(f"Successfully converted JSON data to DataFrame.")
print(df.head())
df.to_csv(OUTPUT_CSV_PATH, index=False)
