from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import from_json, col, count
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configs
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")
db_name = os.getenv("POSTGRES_DB")

# Kafka configs
kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
kafka_topic = os.getenv("KAFKA_TOPIC")

# Checkpoint directory
checkpoint_dir = os.getenv("CHECKPOINT_DIR")

jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars", "/home/sneha-varghese/jars/postgresql-42.6.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType() \
    .add("drug_name", StringType()) \
    .add("manufacturer", StringType()) \
    .add("approval_year", IntegerType()) \
    .add("drug_class", StringType()) \
    .add("indications", StringType()) \
    .add("side_effects", StringType()) \
    .add("dosage_mg", IntegerType()) \
    .add("administration_route", StringType()) \
    .add("contraindications", StringType()) \
    .add("warnings", StringType()) \
    .add("price_usd", DoubleType()) \
    .add("batch_number", StringType()) \
    .add("expiry_date", DateType()) \
    .add("side_effect_severity", StringType()) \
    .add("approval_status", StringType())

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON messages
df_parsed = df_raw.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")

df_parsed.groupBy("manufacturer").agg(count("*").alias("drug_count")).show()

# Logging
logging.basicConfig(level=logging.INFO)

def write_to_postgres(batch_df, batch_id):
    logging.info(f"Processing batch {batch_id}, {batch_df.count()} rows")
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "kafka_stream") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    logging.info(f"Finished batch {batch_id}")

df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_dir) \
    .start() \
    .awaitTermination()

