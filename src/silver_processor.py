import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType

# ─────────────────────────────────────────────────────────────────────────────
# 1. CONFIGURATION & SCHEMAS (Single Source of Truth)
# ─────────────────────────────────────────────────────────────────────────────

BRONZE_PATH = "/data/bronze/events"
COMBINED_CHECKPOINT_PATH = "/data/checkpoints/silver_combined"  # צ'קפוינט מאוחד ונקי

BRONZE_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("actor", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("payload", StringType(), True)
])

ACTOR_STRICT_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("login", StringType(), True)
])

REPO_STRICT_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True)
])

PULL_REQUEST_PAYLOAD_SCHEMA = StructType([
    StructField("action", StringType(), True),
    StructField("pull_request", StructType([
        StructField("id", LongType(), True),
        StructField("merged", BooleanType(), True),
        StructField("user", StructType([StructField("login", StringType(), True)]), True),
        StructField("base", StructType([
            StructField("repo", StructType([StructField("language", StringType(), True)]), True)
        ]), True)
    ]), True)
])

PUSH_PAYLOAD_SCHEMA = StructType([
    StructField("push_id", LongType(), True),
    StructField("commits", ArrayType(StructType([
        StructField("sha", StringType(), True),
        StructField("author", StructType([
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ]), True)
    ])), True)
])

GENERIC_PAYLOAD_SCHEMA = StructType([
    StructField("action", StringType(), True)
])

# ─────────────────────────────────────────────────────────────────────────────
# 2. DATA WRITERS (Sinks)
# ─────────────────────────────────────────────────────────────────────────────

def write_to_postgres(df, table_name: str) -> None:
    pg_host = os.environ.get("PG_HOST", "postgres-server")
    pg_port = os.environ.get("PG_PORT", "5432")
    pg_db = os.environ.get("PG_DB", "crater_analytics")
    pg_user = os.environ.get("PG_USER", "spark")
    pg_password = os.environ.get("PG_PASSWORD", "spark")
    
    postgres_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    df.write \
        .format("jdbc") \
        .option("url", postgres_url) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("batchsize", "1000") \
        .option("rewriteBatchedInserts", "true") \
        .option("isolationLevel", "READ_COMMITTED") \
        .mode("append") \
        .save()

def write_edges_to_neo4j(edges_df) -> None:
    if edges_df.isEmpty():
        return
        
    neo4j_url = os.environ.get("NEO4J_URL", "bolt://neo4j:7687")
    neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
    neo4j_password = os.environ.get("NEO4J_PASSWORD", "neo4jpassword")

    edges_df.write \
        .format("org.neo4j.spark.DataSource") \
        .option("url", neo4j_url) \
        .option("authentication.type", "basic") \
        .option("authentication.basic.username", neo4j_user) \
        .option("authentication.basic.password", neo4j_password) \
        .option("relationship", "CONTRIBUTED_TO") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", "Developer") \
        .option("relationship.target.labels", "Repo") \
        .option("source.keys", "developer") \
        .option("target.keys", "repo_id") \
        .option("target.properties", "repo_name") \
        .mode("append") \
        .save()

# ─────────────────────────────────────────────────────────────────────────────
# 3. TRANSFORMATION SEGMENTS
# ─────────────────────────────────────────────────────────────────────────────

def parse_pull_requests(df):
    return df.filter(col("type") == "PullRequestEvent") \
        .withColumn("parsed_payload", from_json(col("payload"), PULL_REQUEST_PAYLOAD_SCHEMA)) \
        .select(
            col("id").alias("event_id"), col("type"), col("created_at"),
            col("parsed_actor.id").alias("actor_id"), col("parsed_actor.login").alias("actor_login"),
            col("parsed_repo.id").alias("repo_id"), col("parsed_repo.name").alias("repo_name"),
            col("parsed_payload.action").alias("payload_action"),
            col("parsed_payload.pull_request.id").alias("pr_id"),
            col("parsed_payload.pull_request.merged").alias("is_merged"),
            col("parsed_payload.pull_request.user.login").alias("pr_author"),
            col("parsed_payload.pull_request.base.repo.language").alias("repo_language")
        )

def parse_commits(df):
    return df.filter(col("type") == "PushEvent") \
        .withColumn("parsed_payload", from_json(col("payload"), PUSH_PAYLOAD_SCHEMA)) \
        .withColumn("commit", explode(col("parsed_payload.commits"))) \
        .select(
            col("id").alias("event_id"), col("type"), col("created_at"),
            col("parsed_actor.id").alias("pusher_id"), col("parsed_actor.login").alias("pusher_login"),
            col("parsed_repo.id").alias("repo_id"), col("parsed_repo.name").alias("repo_name"),
            col("parsed_payload.push_id").alias("push_id"), 
            col("commit.sha").alias("commit_sha"),
            col("commit.author.name").alias("commit_author_name"),
            col("commit.author.email").alias("commit_author_email")
        )

def extract_graph_edges(parsed_df):
    """חילוץ קצוות ישירות מתוך ה-Base הקיים בלי לקרוא שוב מהדיסק"""
    push_df = parsed_df.filter(col("type") == "PushEvent") \
        .withColumn("parsed_payload", from_json(col("payload"), PUSH_PAYLOAD_SCHEMA)) \
        .withColumn("commit", explode(col("parsed_payload.commits"))) \
        .select(
            col("commit.author.name").alias("developer"),
            col("parsed_repo.id").alias("repo_id"),
            col("parsed_repo.name").alias("repo_name")
        ).filter(col("developer").isNotNull())

    pr_df = parsed_df.filter(col("type") == "PullRequestEvent") \
        .withColumn("parsed_payload", from_json(col("payload"), PULL_REQUEST_PAYLOAD_SCHEMA)) \
        .select(
            col("parsed_payload.pull_request.user.login").alias("developer"),
            col("parsed_repo.id").alias("repo_id"),
            col("parsed_repo.name").alias("repo_name")
        ).filter(col("developer").isNotNull())

    return push_df.union(pr_df).distinct()

def parse_engagement(df):
    return df.filter(col("type").isin(["WatchEvent", "ForkEvent"])) \
        .withColumn("parsed_payload", from_json(col("payload"), GENERIC_PAYLOAD_SCHEMA)) \
        .select(
            col("id").alias("event_id"), col("type"), col("created_at"),
            col("parsed_actor.id").alias("actor_id"), col("parsed_actor.login").alias("actor_login"),
            col("parsed_repo.id").alias("repo_id"), col("parsed_repo.name").alias("repo_name"),
            col("parsed_payload.action").alias("payload_action")
        )

def parse_unknowns(df):
    known_types = ["PullRequestEvent", "PushEvent", "WatchEvent", "ForkEvent"]
    return df.filter(~col("type").isin(known_types)).select(
        col("id").alias("event_id"), col("type"), col("created_at"),
        col("actor").alias("raw_actor_json"), col("repo").alias("raw_repo_json"), col("payload").alias("raw_payload_json")
    )

# ─────────────────────────────────────────────────────────────────────────────
# 4. CENTRAL COMBINED ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def process_combined_pipeline(micro_batch_df, batch_id: int) -> None:
    print(f"\n>>> [Combined Silver Pipeline] Processing Micro-Batch # {batch_id} <<<")
    
    if micro_batch_df.isEmpty():
        return
    print("5"*60)
    # 1. בונים את הבסיס ומבצעים cache לבאץ' הנוכחי בלבד
    enriched_base_df = micro_batch_df \
        .withColumn("parsed_actor", from_json(col("actor"), ACTOR_STRICT_SCHEMA)) \
        .withColumn("parsed_repo", from_json(col("repo"), REPO_STRICT_SCHEMA))
    
    enriched_base_df.cache() 

    # 2. פונקציית עזר לכתיבה + הדפסת שורות
    def write_with_log(df, table_name):
        # חישוב השורות בבאץ' הזה
        count = df.count()
        if count > 0:
            print(f"-> Writing {count} rows to Postgres: {table_name}")
            write_to_postgres(df, table_name)
        else:
            print(f"-> Table {table_name} is empty (0 rows), skipping.")

    # 3. כתיבה לפוסטגרס
    print("-> Starting Postgres Sink...")
    write_with_log(parse_pull_requests(enriched_base_df), "silver_github_pull_requests")
    write_with_log(parse_commits(enriched_base_df), "silver_github_commits")
    write_with_log(parse_engagement(enriched_base_df), "silver_github_engagement")
    write_with_log(parse_unknowns(enriched_base_df), "silver_github_unknown_events")

    # 4. כתיבה ל-Neo4j עם טיפול בשגיאות
    print("-> Starting Neo4j Sink...")
    graph_edges_df = extract_graph_edges(enriched_base_df)
    edge_count = graph_edges_df.count()
    
    if edge_count > 0:
        print(f"-> Writing {edge_count} edges to Neo4j")
        try:
            write_edges_to_neo4j(graph_edges_df)
        except Exception as e:
            # זה ימנע מהנפילה ב-Neo4j להרוס את ה-Checkpoint של הסטרימינג
            print(f"!!! CRITICAL: Neo4j Sink Failed: {e}")
    else:
        print("-> No edges to write to Neo4j, skipping.")

    # 5. ניקוי הזיכרון מיד בסיום הבאץ'
    enriched_base_df.unpersist()

# ─────────────────────────────────────────────────────────────────────────────
# 5. ENGINE ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("CraterSilverEngine") \
        .master("local[4]") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.cores.max", "2") \
        .config("spark.task.cpus", "1") \
        .getOrCreate()

def main() -> None:
    print("="*60)
    print("STARTING COMBINED SILVER ENGINE (POSTGRES + NEO4J)...")
    print("="*60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # קריאת הזרם פעם אחת בלבד מהברונז'
    streaming_raw_df = spark.readStream \
        .schema(BRONZE_SCHEMA) \
        .option("maxFilesPerTrigger", 3) \
        .parquet(BRONZE_PATH)
    
    # 🚀 הרצת סטרים יחיד ומאוחד שמנהל את כל היעדים
    combined_query = streaming_raw_df.writeStream \
        .foreachBatch(process_combined_pipeline) \
        .option("checkpointLocation", COMBINED_CHECKPOINT_PATH) \
        .trigger(processingTime='60 seconds') \
        .start()
    

    print("="*60)
    print("COMBINED ENGINE FINISHED SUCCESSFULLY. MONITORING BATCHES...")
    print("="*60)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()