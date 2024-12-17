import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


def main():
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

    # Define bronze and silver zone paths
    bronze_zone = "bronze/"
    silver_zone = "silver/"
    os.makedirs(silver_zone, exist_ok=True)

    # Define file names
    files = ["athlete_bio", "athlete_event_results"]

    # Register UDF
    clean_text_udf = udf(clean_text, StringType())

    for file in files:
        parquet_path = os.path.join(bronze_zone, f"{file}.parquet")

        print(f"Reading Parquet file from {parquet_path}")
        df = spark.read.parquet(parquet_path)

        # Identify text columns (StringType)
        text_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]
        print(f"Text columns to clean in {file}: {text_columns}")

        # Clean text columns
        for col_name in text_columns:
            print(f"Cleaning column: {col_name}")
            df = df.withColumn(col_name, clean_text_udf(col(col_name)))

        # Remove duplicate rows
        initial_count = df.count()
        df = df.dropDuplicates()
        final_count = df.count()
        print(f"Removed {initial_count - final_count} duplicate rows from {file}")

        # Coalesce to 1 partition to ensure single CSV file
        df = df.coalesce(1)

        # Write to silver zone in CSV
        csv_path = os.path.join(silver_zone, f"{file}.csv")
        print(f"Writing cleaned data to {csv_path}")
        df.write.mode("overwrite") \
            .option("header", "true") \
            .option("escape", "\"") \
            .csv(csv_path)
        print(f"Saved cleaned {file}.csv to {csv_path}\n")


    spark.stop()

if __name__ == "__main__":
    main()
