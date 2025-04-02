import pandas as pd
import random
import os
import math

# --- Configuration ---
input_csv_path = (
    "./airflow/input/tracks_features.csv"  # <--- CHANGE THIS to your input file path
)
output_dir = (
    "./airflow/input/chunked/"  # <--- CHANGE THIS (optional) Output directory name
)
min_rows_per_file = 10000
max_rows_per_file = 200000
# --- End Configuration ---

print(f"Starting script...")

# --- 0. Create output directory if it doesn't exist ---
os.makedirs(output_dir, exist_ok=True)
print(f"Output directory '{output_dir}' ensured.")

# --- 1. Load the CSV ---
try:
    print(f"Loading {input_csv_path} into memory...")
    # Consider adding dtype={'col_name': str} if you have mixed type warnings
    # Consider adding low_memory=False if you encounter dtype issues
    df = pd.read_csv(input_csv_path)
    total_rows = len(df)
    print(f"Successfully loaded {total_rows:,} rows.")
    if total_rows == 0:
        print("Input file is empty. Exiting.")
        exit()
except FileNotFoundError:
    print(f"Error: Input file not found at '{input_csv_path}'")
    exit()
except Exception as e:
    print(f"Error loading CSV: {e}")
    exit()

# --- 2. Shuffle Indices ---
print("Shuffling row indices...")
indices = list(df.index)  # Get a list of indices [0, 1, 2, ..., n-1]
random.shuffle(indices)  # Shuffle the list of indices randomly in place
print("Indices shuffled.")

# --- 3. Iterate and Split ---
current_index_pos = 0
file_counter = 1
print("Starting splitting process...")

while current_index_pos < total_rows:
    # Determine the size of the next chunk
    chunk_size = random.randint(min_rows_per_file, max_rows_per_file)

    # Ensure chunk_size doesn't exceed remaining rows
    remaining_rows = total_rows - current_index_pos
    actual_chunk_size = min(chunk_size, remaining_rows)

    # Get the slice of shuffled indices for this chunk
    end_index_pos = current_index_pos + actual_chunk_size
    chunk_indices = indices[current_index_pos:end_index_pos]

    # Select the rows from the original DataFrame using the shuffled indices
    df_chunk = df.iloc[chunk_indices]

    # Define output filename (e.g., output_split_files/split_001.csv)
    # Using padding in the filename helps with sorting later
    output_filename = os.path.join(output_dir, f"split_{file_counter:03d}.csv")

    # Save the chunk to a new CSV
    print(
        f"  Saving file {file_counter}: {output_filename} ({len(df_chunk):,} rows)..."
    )
    try:
        df_chunk.to_csv(
            output_filename, index=False
        )  # index=False prevents writing pandas index
    except Exception as e:
        print(f"    Error saving {output_filename}: {e}")
        # Decide if you want to stop or continue if a save fails
        # exit() or continue

    # Update position and counter for the next loop iteration
    current_index_pos = end_index_pos
    file_counter += 1

print("\nSplitting complete.")
print(f"Total rows processed: {current_index_pos:,}")
print(f"Number of files created: {file_counter - 1}")
print(f"Output files are located in: '{output_dir}'")
