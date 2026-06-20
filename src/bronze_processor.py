import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION & INITIAL LAZY SCHEMA
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "gh-archive-events"
OUTPUT_PATH = "/data/bronze/events"
CHECKPOINT_PATH = "/data/checkpoints/events"

# הסכמה הראשונית לתוכן לפני החלוקה - שומרת על השדות המורכבים כסטרינג גולמי
INITIAL_EVENT_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("actor", StringType(), True),   # נשמר כ-JSON גולמי
    StructField("repo", StringType(), True),    # נשמר כ-JSON גולמי
    StructField("payload", StringType(), True)  # נשמר כ-JSON גולמי
])

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CraterBronzeProcessor") \
        .master("local[6]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

def read_kafka_stream(spark: SparkSession, servers: str, topic: str):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 5000) \
        .load()

def transform_kafka_to_initial_bronze(raw_df):
    """מחלץ את הסכמה הראשונית והשטוחה מתוך ה-Value של קפקא"""
    return raw_df \
        .withColumn("json_str", col("value").cast("string")) \
        .withColumn("data", from_json(col("json_str"), INITIAL_EVENT_SCHEMA)) \
        .select("data.*")

def write_bronze_stream(parsed_df, output_path: str, checkpoint_path: str):
    return parsed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    raw_stream = read_kafka_stream(spark, KAFKA_SERVERS, TOPIC)
    parsed_stream = transform_kafka_to_initial_bronze(raw_stream)
    query = write_bronze_stream(parsed_stream, OUTPUT_PATH, CHECKPOINT_PATH)
    
    query.awaitTermination()

if __name__ == "__main__":
    main()