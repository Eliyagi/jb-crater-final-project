import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION & SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "gh-archive-events"
OUTPUT_PATH = "/data/bronze/events"
CHECKPOINT_PATH = "/data/checkpoints/events"

# BASE_SCHEMA = StructType([
#     StructField("id", StringType(), True),
#     StructField("type", StringType(), True),
#     StructField("created_at", StringType(), True),
#     StructField("payload", StringType(), True)
# ])

BASE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("actor", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("payload", StringType(), True)
])

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """Initializes and returns the Spark session."""
    return SparkSession.builder \
        .appName("CraterSparkEngine") \
        .master("local[6]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


def read_kafka_stream(spark: SparkSession, servers: str, topic: str):
    """Reads streaming data from the specified Kafka topic."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()


def transform_events(raw_df):
    """Decodes UTF-8 bytes and deserializes JSON using the schema."""
    return raw_df.select(
        from_json(col("value").cast("string"), BASE_SCHEMA).alias("data")
    ).select("data.*")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # 1. איתחול
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # 2. קריאה (Extract)
    raw_stream = read_kafka_stream(spark, KAFKA_SERVERS, TOPIC)
    
    # 3. טרנספורמציה (Transform)
    parsed_stream = transform_events(raw_stream)
    
    # # 4. בדיקה זמנית למסך (Load Console)
    # query = parsed_stream.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .start()
        
    query = parsed_stream.writeStream \
        .format("parquet") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .outputMode("append") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()