import os
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark import StorageLevel

# Import our custom schemas and transformation functions
from transformations import (
    transform_pr_events,
    transform_push_events,
    transform_watch_events,
    transform_fork_events
)

# 1. משיכת קונפיגורציות ממשתני הסביבה של הדוקר
KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gh-archive-events")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "crater_analytics")
PG_USER = os.getenv("PG_USER", "spark")
PG_PASSWORD = os.getenv("PG_PASSWORD", "spark")

NEO4J_URL = os.getenv("NEO4J_URL", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "neo4jpassword")

# 2. אתחול ה-SparkSession עם הגדרות אופטימיזציה ללפטופ
spark = SparkSession.builder \
    .appName("CraterKafkaStreamingPipeline") \
    .master("local[2]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 3. הגדרת סכמה בסיסית לשדות העליונים של ה-JSON מקפקא
kafka_json_schema = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("created_at", T.TimestampType(), True),
    T.StructField("actor", T.StringType(), True),
    T.StructField("repo", T.StringType(), True),
    T.StructField("payload", T.StringType(), True)
])

# 4. פונקציית הקסם - מעבדת כל מיקרו-באטץ' שמגיע מקפקא
def process_micro_batch(batch_df, batch_id):
    # Action ראשון ולגיטימי - אם הבאטץ' ריק לחלוטין, יוצאים מיד בלי לבצע כלום
    if batch_df.isEmpty():
        return

    print(f"\n[Spark Stream] ⚡ Processing batch {batch_id}...")

    # 1. פירוק ראשוני קל ושמירה על ה-Dataframe הכללי
    parsed_df = batch_df \
        .withColumn("json_str", F.col("value").cast("string")) \
        .withColumn("data", F.from_json(F.col("json_str"), kafka_json_schema)) \
        .select("data.*") \
        .withColumn("actor_login", F.get_json_object(F.col("actor"), "$.login")) \
        .withColumn("repo_name", F.get_json_object(F.col("repo"), "$.name"))
        
    # קאש קריטי - מונע מספארק לחזור לקפקא עבור כל יעד כתיבה (Sink)
    parsed_df.persist(StorageLevel.MEMORY_AND_DISK_2)
    
    try:
        # ───────────────────────────────────────────────────────────────────
        # שלב א': כתיבה ל-PARQUET (DATA LAKE) - קודם כל שומרים את המקור
        # ───────────────────────────────────────────────────────────────────
        parquet_path = "/app/code/data_lake/events"
        parsed_df.write \
            .mode("append") \
            .parquet(parquet_path)
        print(f"[Spark Stream] 💾 Saved raw batch to Parquet Data Lake.")

        # ───────────────────────────────────────────────────────────────────
        # שלב ב': כתיבה ל-POSTGRESQL (4 טבלאות נפרדות בצורה אופטימלית)
        # ───────────────────────────────────────────────────────────────────
        pg_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
        
        # 1. טבלת Pull Requests
        pr_df = transform_pr_events(parsed_df)
        pr_df.write.format("jdbc") \
            .option("url", pg_url).option("dbtable", "pull_requests") \
            .option("user", PG_USER).option("password", PG_PASSWORD) \
            .option("numPartitions", "1").mode("append").save()

        # 2. טבלת Pushes
        push_df = transform_push_events(parsed_df)
        push_df.write.format("jdbc") \
            .option("url", pg_url).option("dbtable", "pushes") \
            .option("user", PG_USER).option("password", PG_PASSWORD) \
            .option("numPartitions", "1").mode("append").save()

        # 3. טבלת Watches (Stars)
        watch_df = transform_watch_events(parsed_df)
        watch_df.write.format("jdbc") \
            .option("url", pg_url).option("dbtable", "watches") \
            .option("user", PG_USER).option("password", PG_PASSWORD) \
            .option("numPartitions", "1").mode("append").save()

        # 4. טבלת Forks
        fork_df = transform_fork_events(parsed_df)
        fork_df.write.format("jdbc") \
            .option("url", pg_url).option("dbtable", "forks") \
            .option("user", PG_USER).option("password", PG_PASSWORD) \
            .option("numPartitions", "1").mode("append").save()
            
        print("[Spark Stream] 🏛️ Postgres tables updated (PRs, Pushes, Watches, Forks).")

        # ───────────────────────────────────────────────────────────────────
        # שלב ג': כתיבה ל-NEO4J (קשרים חברתיים - שאלות 3 ו-5)
        # ───────────────────────────────────────────────────────────────────
        edges_df = parsed_df.filter(F.col("type").isin("PushEvent", "PullRequestEvent")).select(
            F.col("actor_login"), F.col("repo_name")
        ).distinct()

        print("\n" + "="*60)
        print("🚀 [DEBUG] SPARK IS NOW WRITING TO NEO4J...")
        print("="*60 + "\n")

        edges_df.write \
            .format("org.neo4j.spark.DataSource") \
            .option("url", NEO4J_URL) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", NEO4J_USER) \
            .option("authentication.basic.password", NEO4J_PASSWORD) \
            .option("query", """
                MERGE (a:Actor {login: event.actor_login})
                MERGE (r:Repository {name: event.repo_name})
                MERGE (a)-[:CONTRIBUTED_TO]->(r)
            """) \
            .mode("Append") \
            .save()
        
        print(f"[Spark Stream] 🕸️ Social network graph updated in Neo4j.")
        
    except Exception as e:
        print(f"[Spark Stream] ❌ Error in batch {batch_id}: {str(e)}")

    finally:
        # ניקוי הזיכרון בסוף האיטרציה - חובה למניעת OutOfMemory (OOM)
        parsed_df.unpersist()
        print(f"[Spark Stream] ✨ Batch {batch_id} completed and memory cleared.")

# 5. הגדרת מקור הסטרימינג - קריאה מקפקא
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", 5000) \
    .load()

# 6. הפעלת הזרם (Streaming Query) באמצעות מנגנון foreachBatch
query = kafka_stream_df.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "/app/code/checkpoints/spark_kafka") \
    .start()

print(f"[Spark Engine] 🎬 סטרימינג מקפקא באוויר! מאזין לטופיק: {KAFKA_TOPIC}...")
query.awaitTermination()