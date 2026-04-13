import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, current_timestamp, unix_timestamp, lit, struct
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType

# Kafka configuration (from environment variables)
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_TOPIC_IN = os.getenv("KAFKA_TOPIC_IN")
KAFKA_TOPIC_OUT = os.getenv("KAFKA_TOPIC_OUT")

# Kafka SSL configuration (Java truststore)
# If not set, the default JVM truststore is used
KAFKA_SSL_TRUSTSTORE_LOCATION = os.getenv("KAFKA_SSL_TRUSTSTORE_LOCATION", "")
KAFKA_SSL_TRUSTSTORE_PASSWORD = os.getenv("KAFKA_SSL_TRUSTSTORE_PASSWORD", "")

# Checkpoint directory (stores offsets across restarts)
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/home/ajdaralijev/spark-checkpoints/restaurant-streaming")

# PostgreSQL configuration (source of subscriber data)
PG_SOURCE_HOST = os.getenv("PG_SOURCE_HOST")
PG_SOURCE_PORT = os.getenv("PG_SOURCE_PORT", "6432")
PG_SOURCE_DB = os.getenv("PG_SOURCE_DB")
PG_SOURCE_USER = os.getenv("PG_SOURCE_USER")
PG_SOURCE_PASSWORD = os.getenv("PG_SOURCE_PASSWORD")

# PostgreSQL configuration (destination for feedback writes)
PG_DEST_HOST = os.getenv("PG_DEST_HOST")
PG_DEST_PORT = os.getenv("PG_DEST_PORT", "5432")
PG_DEST_DB = os.getenv("PG_DEST_DB")
PG_DEST_USER = os.getenv("PG_DEST_USER")
PG_DEST_PASSWORD = os.getenv("PG_DEST_PASSWORD")

# Validate required variables
required_kafka = [KAFKA_BOOTSTRAP_SERVER, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT]
required_pg_source = [PG_SOURCE_HOST, PG_SOURCE_DB, PG_SOURCE_USER, PG_SOURCE_PASSWORD]
required_pg_dest = [PG_DEST_HOST, PG_DEST_DB, PG_DEST_USER, PG_DEST_PASSWORD]

if not all(required_kafka):
    raise ValueError("The Kafka environment variables must be set: KAFKA_BOOTSTRAP_SERVER, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT")
if not all(required_pg_source):
    raise ValueError("The source PostgreSQL environment variables must be set: PG_SOURCE_HOST, PG_SOURCE_DB, PG_SOURCE_USER, PG_SOURCE_PASSWORD")
if not all(required_pg_dest):
    raise ValueError("The destination PostgreSQL environment variables must be set: PG_DEST_HOST, PG_DEST_DB, PG_DEST_USER, PG_DEST_PASSWORD")

# Libraries required to integrate Spark with Kafka and PostgreSQL
spark_jars_packages = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    "org.postgresql:postgresql:42.4.0",
])


def get_kafka_options():
    """
    Returns a dict of options for connecting to Kafka.
    Adds the SSL truststore when it is specified in the environment variables.
    """
    options = {
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVER,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";',
        "kafka.sasl.mechanism": "SCRAM-SHA-512",
    }

    # Add the truststore when specified (for environments where the CA is not in the JVM)
    if KAFKA_SSL_TRUSTSTORE_LOCATION and KAFKA_SSL_TRUSTSTORE_PASSWORD:
        options["kafka.ssl.truststore.location"] = KAFKA_SSL_TRUSTSTORE_LOCATION
        options["kafka.ssl.truststore.password"] = KAFKA_SSL_TRUSTSTORE_PASSWORD

    return options


# Create the Spark session
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Retrieve Kafka options
kafka_options = get_kafka_options()

# Read restaurant promotional campaign messages from the Kafka topic
restaurant_read_stream_df = spark.readStream \
    .format("kafka") \
    .options(**kafka_options) \
    .option("subscribe", KAFKA_TOPIC_IN) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Define the schema of the incoming JSON message
incoming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True),
])

# Deserialize JSON from the value field and convert it into a DataFrame with named columns
parsed_df = restaurant_read_stream_df \
    .select(
        from_json(col("value").cast("string"), incoming_message_schema).alias("parsed_value")
    ) \
    .select("parsed_value.*")

# Filter campaigns by time: the current time must fall between the campaign start and end
# Get the current time in seconds (Unix timestamp)
current_timestamp_utc = unix_timestamp(current_timestamp())

filtered_df = parsed_df.filter(
    (col("adv_campaign_datetime_start") <= current_timestamp_utc) &
    (col("adv_campaign_datetime_end") > current_timestamp_utc)
)

# Read subscriber data from PostgreSQL
# No show() or count() calls - they add unnecessary overhead
subscribers_restaurant_df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{PG_SOURCE_HOST}:{PG_SOURCE_PORT}/{PG_SOURCE_DB}") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "subscribers_restaurants") \
    .option("user", PG_SOURCE_USER) \
    .option("password", PG_SOURCE_PASSWORD) \
    .load()

# Cache the subscribers table - it is reused in every micro-batch
subscribers_restaurant_df.cache()

# Log only the fact that the table was loaded, without count()
print("=== Subscribers table loaded from PostgreSQL ===")


def foreach_batch_function(df, epoch_id):
    """
    Function for processing each micro-batch of the stream.
    Writes data to PostgreSQL (with feedback) and Kafka (without feedback).

    checkpointLocation persists offsets across restarts,
    preventing messages from being reprocessed.
    """
    # Check whether the batch has any data (without a full count)
    if df.rdd.isEmpty():
        print(f"=== Batch {epoch_id}: empty batch, skipping ===")
        return

    # Cache the DataFrame for reuse
    df.persist()

    try:
        # 1. Write to PostgreSQL for analytics (with the feedback column)
        df_with_feedback = df.withColumn("feedback", lit(None).cast(StringType()))

        pg_url = f"jdbc:postgresql://{PG_DEST_HOST}:{PG_DEST_PORT}/{PG_DEST_DB}"

        df_with_feedback.write \
            .format("jdbc") \
            .option("url", pg_url) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "subscribers_feedback") \
            .option("user", PG_DEST_USER) \
            .option("password", PG_DEST_PASSWORD) \
            .mode("append") \
            .save()

        # 2. Send to Kafka for push notifications (without the feedback column)
        df.select(
            to_json(struct(col("*"))).alias("value")
        ).write \
            .format("kafka") \
            .options(**kafka_options) \
            .option("topic", KAFKA_TOPIC_OUT) \
            .save()

        # Log without count() - only the fact that the write succeeded
        print(f"=== Batch {epoch_id}: data written to PostgreSQL and Kafka ===")

    except Exception as e:
        print(f"=== Batch {epoch_id}: ERROR - {str(e)} ===")
        raise  # Propagate the exception to trigger Spark's retry mechanism

    finally:
        # Release the cache
        df.unpersist()


# Join data from Kafka with subscribers from PostgreSQL on restaurant_id
# Add the trigger_datetime_created column with the current time
result_df = filtered_df.join(
    subscribers_restaurant_df,
    filtered_df.restaurant_id == subscribers_restaurant_df.restaurant_id,
    "inner"
).select(
    # Select columns without duplicating restaurant_id
    filtered_df.restaurant_id,
    filtered_df.adv_campaign_id,
    filtered_df.adv_campaign_content,
    filtered_df.adv_campaign_owner,
    filtered_df.adv_campaign_owner_contact,
    filtered_df.adv_campaign_datetime_start,
    filtered_df.adv_campaign_datetime_end,
    filtered_df.datetime_created,
    subscribers_restaurant_df.client_id,
    # Cast to IntegerType to match int4 in PostgreSQL
    unix_timestamp(current_timestamp()).cast(IntegerType()).alias("trigger_datetime_created")
)

# Launch the stream that writes to PostgreSQL via foreachBatch
# FIXED: added checkpointLocation to persist offsets
query = result_df.writeStream \
    .outputMode("append") \
    .foreachBatch(foreach_batch_function) \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

print(f"=== Stream started, checkpoint: {CHECKPOINT_LOCATION} ===")

query.awaitTermination()
