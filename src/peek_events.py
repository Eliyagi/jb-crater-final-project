import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "gh-archive-events"

BASE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("payload", StringType(), True)
])

def main():
    # איתחול ספארק במצב Batch (לא סטרימינג) לצורך בדיקה
    spark = SparkSession.builder \
        .appName("CraterPeek") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # קריאה חד פעמית מקפקא (read במקום readStream)
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    # פירוק ה-JSON והצגת 5 ההודעות הראשונות
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), BASE_SCHEMA).alias("data")
    ).select("data.*")

    # הצגת הנתונים בצורה אנכית ומורחבת (truncate=False מראה הכל)
    parsed_df.show(5, truncate=False, vertical=True)

if __name__ == "__main__":
    main()