import os

from pyspark.sql import SparkSession
from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config
from pyspark.sql.functions import from_json, broadcast
from pyspark.sql.functions import col, to_json, struct, from_json, when, lit, trim, lower, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType

# configuration
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_table_bio = "athlete_bio"
jdbc_table_event_results = "athlete_event_results"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
mysql_driver = "com.mysql.cj.jdbc.Driver"

os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

'=========================TASK 1========================='

# Initialize Spark Session with MySQL connector
spark = SparkSession.builder \
    .appName("MySQLToKafka") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

# Read athlete_bio data from MySQL
athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=mysql_driver,
    dbtable=jdbc_table_bio,
    user=jdbc_user,
    password=jdbc_password
).load()

# Display data
athlete_bio_df.show(5)

'=========================TASK 2========================='

filtered_athlete_bio_df = athlete_bio_df \
    .filter(
        (col("height").isNotNull()) &
        (col("weight").isNotNull()) &
        (col("height").cast("double").isNotNull()) &
        (col("weight").cast("double").isNotNull())
    )

# display how many records were filtered out
print("Total records before filtering:", athlete_bio_df.count())
print("Total records after filtering:", filtered_athlete_bio_df.count())

#filtered_athlete_bio_df.show(5)

'=========================TASK 3========================='

# create Kafka client
admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

# create topics
new_topics = [
    NewTopic(name='athlete_event_results_iuliia_2', num_partitions=2, replication_factor=1),
    NewTopic(name='athlete_event_results_iuliia_enriched', num_partitions=2, replication_factor=1),
]

try:
    existing_topics = admin_client.list_topics()
    topics_to_create = [topic for topic in new_topics if topic.name not in existing_topics]
    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Topics '{', '.join([topic.name for topic in topics_to_create])}' created successfully.")
    else:
        print("Topics already exist. Skipping creation.")
except Exception as e:
    print(f"An error occurred while creating topics: {e}")

# Read athlete_event_results from MySQL
athlete_event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver=mysql_driver,
    dbtable=jdbc_table_event_results,
    user=jdbc_user,
    password=jdbc_password
).load()

athlete_event_results_df.show(5)

athlete_event_results_df = athlete_event_results_df.withColumn("isTeamSport", trim(col("isTeamSport")))
athlete_event_results_df = athlete_event_results_df.withColumn("isTeamSport", lower(col("isTeamSport")))
athlete_event_results_df = athlete_event_results_df.withColumn(
    "isTeamSport",
    when(col("isTeamSport") == "false", lit(False))
    .when(col("isTeamSport") == "true", lit(True))
    .otherwise(lit(None).cast("boolean"))
)

# Convert rows to JSON and write to Kafka
athlete_event_results_json_df = athlete_event_results_df.select(to_json(struct("*")).alias("value"))
athlete_event_results_json_df.show(5, truncate=False)

athlete_event_results_json_df \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", 'athlete_event_results_iuliia_2') \
    .save()

print("Data from athlete_event_results_df has been successfully written to Kafka topic:", 'athlete_event_results_iuliia_2')

# read the data back from the Kafka topic and parse the JSON
athlete_event_results_kafka_df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("subscribe", 'athlete_event_results_iuliia_2') \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .load()

athlete_event_results_parsed = athlete_event_results_kafka_df.selectExpr("CAST(value AS STRING) as json_value")

# schema for the athlete_event_results data
event_results_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True)
])

athlete_event_results_kafka_df.selectExpr("CAST(value AS STRING) as raw_value").show(n=20, truncate=False)

parsed_event_results_df = athlete_event_results_parsed.withColumn("data", from_json(col("json_value"), event_results_schema)) \
    .select("data.*")

print("Parsed athlete_event_results from Kafka:")
parsed_event_results_df.show(5, truncate=False)

'=========================TASK 4========================='

# Perform inner join on athlete_id key
joined_df = parsed_event_results_df.join(
    filtered_athlete_bio_df,
    parsed_event_results_df["athlete_id"] == filtered_athlete_bio_df["athlete_id"],
    "inner"
)

joined_df.show(5, truncate=15)

'=========================TASK 5========================='

joined_selected_df = joined_df.select(
    parsed_event_results_df["sport"],
    parsed_event_results_df["medal"],
    parsed_event_results_df["country_noc"],  # take from event results
    filtered_athlete_bio_df["height"],
    filtered_athlete_bio_df["weight"],
    filtered_athlete_bio_df["sex"]
)

result_df = joined_selected_df.groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("calculation_timestamp", current_timestamp())

result_df.show(truncate=False)

'=========================TASK 6========================='

# Create a streaming DataFrame from the Kafka topic
athlete_event_results_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("subscribe", 'athlete_event_results_iuliia_2') \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .load()

# Convert the Kafka value from binary to string
athlete_event_results_parsed_stream_df = athlete_event_results_stream_df.selectExpr("CAST(value AS STRING) as json_value")

# Define the schema for event results
event_results_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", BooleanType(), True)
])

parsed_event_results_stream_df = athlete_event_results_parsed_stream_df \
    .withColumn("data", from_json(col("json_value"), event_results_schema)) \
    .select("data.*")

# Join the streaming DF with the static filtered_athlete_bio_df
filtered_athlete_bio_df_renamed = filtered_athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")
filtered_athlete_bio_df_renamed.show(5, truncate=15)

joined_df = parsed_event_results_stream_df.join(
    filtered_athlete_bio_df_renamed,
    "athlete_id",
    "inner"
)

joined_selected_df = joined_df.select(
    "sport",
    "medal",
    "bio_country_noc",
    "height",
    "weight",
    "sex"
)

# Perform aggregation
result_df = joined_selected_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
    .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight")
    ) \
    .withColumn("calculation_timestamp", current_timestamp())


# Convert the result_stream_df rows into JSON
enriched_for_kafka_df = result_df \
    .select(to_json(struct("*")).alias("value"))

final_df = result_df.withColumnRenamed("bio_country_noc", "country_noc") # Rename back for MySQL

# Function to write each micro-batch to both Kafka and MySQL
def foreach_batch_function(batch_df, batch_id):
    # Write to Kafka
    (batch_df
        .select(to_json(struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
        .option("kafka.security.protocol", kafka_config['security_protocol'])
        .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
        .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
        .option("topic", "athlete_event_results_iuliia_enriched")
        .save()
    )

    # Write to MySQL
    (batch_df
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "iuliia_athlete_event_results_summary")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .mode("append")
        .save()
    )

# Add a console sink for debugging (Print data from the Kafka stream)
console_query = (result_df
    .withColumnRenamed("bio_country_noc", "country_noc")  
    .writeStream
    .outputMode("complete")  
    .format("console")  
    .option("truncate", "false")
    .start()
)

# Start the streaming query and apply foreachBatch
query = (final_df
    .writeStream
    .outputMode("complete")   
    .foreachBatch(foreach_batch_function)
    .start()
)

# Await termination for both queries
spark.streams.awaitAnyTermination()


