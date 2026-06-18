import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION & SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "gh-archive-events"

# סכמה פנימית עבור מחבר הקומיט
AUTHOR_SCHEMA = StructType([
    StructField("email", StringType(), True),
    StructField("name", StringType(), True)
])

# סכמה פנימית עבור אובייקט קומיט בודד
COMMIT_SCHEMA = StructType([
    StructField("sha", StringType(), True),
    StructField("message", StringType(), True),
    StructField("author", AUTHOR_SCHEMA, True)
])

# סכמה עבור ה-Payload המלא של ה-Push
PAYLOAD_SCHEMA = StructType([
    StructField("ref", StringType(), True),
    StructField("commits", ArrayType(COMMIT_SCHEMA), True)
])

# הסכמה הראשית של קפקא (נוספה עמודת ה-payload המפורקת)
BASE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("payload", StringType(), True)  # נשאר זמנית כטקסט לצורך הפירוק הבא
])

# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CraterSparkEngine") \
        .master("local[*]") \
        .getOrCreate()

def read_kafka_stream(spark: SparkSession, servers: str, topic: str):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def transform_events(raw_df):
    """מפרק את ה-JSON הראשי ואז מפרק את ה-JSON הפנימי של ה-payload"""
    # 1. פירוק ראשוני של ההודעה מקפקא
    parsed_base = raw_df.select(
        from_json(col("value").cast("string"), BASE_SCHEMA).alias("data")
    ).select("data.*")
    
    # 2. פירוק ה-JSON המורכב שנמצא בתוך שדה ה-payload
    final_df = parsed_base.withColumn(
        "parsed_payload", 
        from_json(col("payload"), PAYLOAD_SCHEMA)
    ).select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        col("created_at"),
        col("parsed_payload.ref").alias("branch"),
        col("parsed_payload.commits").alias("commits")
    )
    
    return final_df