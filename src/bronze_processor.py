import os
import sys
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
    # 🌟 תיקון: שינוי ל-local[2] והתאמת הזיכרון למגבלות ה-Compose
    return SparkSession.builder \
        .appName("CraterBronzeProcessor") \
        .master("local[1]") \
        .config("spark.driver.memory", "512m") \
        .config("spark.executor.memory", "450m") \
        .config("spark.executor.memoryOverhead", "62m") \
        .config("spark.cores.max", "1") \
        .config("spark.task.cpus", "1") \
        .getOrCreate()

def read_kafka_stream(spark: SparkSession, servers: str, topic: str):
    # 🌟 תיקון: הגבלת האופסטים לטריגר למניעת הכתבה וקריסה (OOM)
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 1500) \
        .load()

def transform_kafka_to_initial_bronze(raw_df):
    """מחלץ את הסכמה הראשונית והשטוחה מתוך ה-Value של קפקא"""
    return raw_df \
        .withColumn("json_str", col("value").cast("string")) \
        .withColumn("data", from_json(col("json_str"), INITIAL_EVENT_SCHEMA)) \
        .select("data.*")

def write_bronze_stream(parsed_df, output_path: str, checkpoint_path: str):
    # 🌟 תיקון: הוספת Trigger מבוסס זמן למניעת יצירת קבצי Parquet קטנים מדי
    return parsed_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("="*60)
    print("STARTING BRONZE ENGINE (KAFKA -> PARQUET)...")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    raw_stream = read_kafka_stream(spark, KAFKA_SERVERS, TOPIC)
    parsed_stream = transform_kafka_to_initial_bronze(raw_stream)
    query = write_bronze_stream(parsed_stream, OUTPUT_PATH, CHECKPOINT_PATH)
    
    print("BRONZE STREAMING QUERY STARTED SUCCESSFULLY.")
    query.awaitTermination()

if __name__ == "__main__":
    main()