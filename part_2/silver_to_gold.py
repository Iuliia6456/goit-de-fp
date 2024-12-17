import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp


def main():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    # Define silver and gold zone paths
    silver_zone = "silver/"
    gold_zone = "gold/"
    os.makedirs(gold_zone, exist_ok=True)

    # Define directory paths
    athlete_bio_dir = os.path.join(silver_zone, "athlete_bio.csv")
    athlete_event_results_dir = os.path.join(silver_zone, "athlete_event_results.csv")

    # Read athlete_bio CSV files from directory
    athlete_bio_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(athlete_bio_dir)

    # Read athlete_event_results CSV files from directory
    athlete_event_results_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(athlete_event_results_dir)

    # Display sample data for verification
    print("Sample athlete_bio_df:")
    athlete_bio_df.show(5)

    print("Sample athlete_event_results_df:")
    athlete_event_results_df.show(5)

    # Rename 'country_noc' in athlete_bio_df to eliminate ambiguity
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Ensure numeric columns are correctly typed
    athlete_bio_df = athlete_bio_df.withColumn("height", col("height").cast("double")) \
        .withColumn("weight", col("weight").cast("double"))

    # Perform inner join on athlete_id
    joined_df = athlete_event_results_df.join(
        athlete_bio_df,
        on="athlete_id",
        how="inner"
    )

    # Select necessary columns, including bio_country_noc
    selected_df = joined_df.select(
        "sport",
        "medal",
        "bio_country_noc",  # Temporarily renamed column
        "height",
        "weight",
        "sex"
    )

    # Rename 'bio_country_noc' back to 'country_noc'
    selected_df = selected_df.withColumnRenamed("bio_country_noc", "country_noc")

    # Perform aggregations
    result_df = selected_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
        .withColumn("calculation_timestamp", current_timestamp())

    # Display the aggregated result
    print("Aggregated Result:")
    result_df.show(truncate=False)

    result_df = result_df.coalesce(1)

    # Write the aggregated result to the gold zone in CSV format
    gold_path = os.path.join(gold_zone, "athlete_event_results_summary.csv")
    result_df.write.mode("overwrite") \
        .option("header", "true") \
        .option("escape", "\"") \
        .csv(gold_path)


    print(f"Saved analytical data to {gold_path}")

    spark.stop()


if __name__ == "__main__":
    main()
