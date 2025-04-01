from pyspark.sql import SparkSession
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MinIO Read Write Example").getOrCreate()

    # Ensure S3A settings are configured via spark-defaults, connection extras, or env vars
    print(f"Reading from: {args.input_path}")
    df = spark.read.csv(args.input_path, header=True, inferSchema=True)

    print("Processing data...")
    df_processed = df.filter(df['some_column'] > 10) # Example processing

    print(f"Writing to: {args.output_path}")
    df_processed.write.mode("overwrite").parquet(args.output_path)

    print("Job finished.")
    spark.stop()