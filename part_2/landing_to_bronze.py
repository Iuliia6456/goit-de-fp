import os
import requests
from pyspark.sql import SparkSession


def download_data(file_name, local_dir):
    base_url = "https://ftp.goit.study/neoversity/"
    downloading_url = f"{base_url}{file_name}.csv"
    local_file_path = os.path.join(local_dir, f"{file_name}.csv")

    print(f"Downloading from {downloading_url}")
    response = requests.get(downloading_url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Ensure the local directory exists
        os.makedirs(local_dir, exist_ok=True)

        # Write the content to the local file
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        exit(f"Failed to download the file. Status code: {response.status_code}")


def main():
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .getOrCreate()

    # Define file names
    files = ["athlete_bio", "athlete_event_results"]

    # Define landing zone paths
    landing_zone = "bronze/"

    for file in files:
        # Download the CSV file to the landing zone
        download_data(file, landing_zone)

        # Read CSV using Spark
        csv_path = os.path.join(landing_zone, f"{file}.csv")
        print(f"Reading CSV file from {csv_path}")
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(csv_path)

        # Write to Parquet
        parquet_path = os.path.join(landing_zone, f"{file}.parquet")
        df.write.mode("overwrite").parquet(parquet_path)
        print(f"Saved {file}.parquet to {parquet_path}")

    spark.stop()


if __name__ == "__main__":
    main()
