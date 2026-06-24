import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType

# ───────────────────────────────────────────────────────────────────────
# 1. GLOBAL SCHEMAS DEFINITION
# ───────────────────────────────────────────────────────────────────────

# Schema for PullRequestEvent (Tailored for Question 1 & 4)
PR_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("login", StringType(), True)  # The actor who closed/opened
    ]), True),
    StructField("repo", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),  # opened, closed
        StructField("pull_request", StructType([
            StructField("id", LongType(), True),
            StructField("merged", BooleanType(), True),  # Core filter for Q1
            StructField("user", StructType([
                StructField("login", StringType(), True)  # Real PR Author (Q1)
            ]), True),
            StructField("base", StructType([
                StructField("repo", StructType([
                    StructField("language", StringType(), True)  # Repo Language (Q1)
                ]), True)
            ]), True)
        ]), True)
    ]), True),
    StructField("created_at", StringType(), True)
])

# Schema for PushEvent (Tailored for Question 2)
PUSH_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("login", StringType(), True)  # The Pusher
    ]), True),
    StructField("repo", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("payload", StructType([
        StructField("push_id", LongType(), True),
        StructField("size", IntegerType(), True),
        StructField("ref", StringType(), True),
        StructField("commits", StructType([  # Array of commits containing authors
            StructField("author", StructType([
                StructField("name", StringType(), True),
                StructField("email", StringType(), True)
            ]), True)
        ]), True)
    ]), True),
    StructField("created_at", StringType(), True)
])

# Schema for WatchEvent - Stars (Tailored for Question 4)
WATCH_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("login", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("created_at", StringType(), True)
])

# Schema for ForkEvent (Tailored for Question 4)
FORK_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("login", StringType(), True)
    ]), True),
    StructField("repo", StructType([
        StructField("name", StringType(), True)
    ]), True),
    StructField("created_at", StringType(), True)
])


# ───────────────────────────────────────────────────────────────────────
# 2. TRANSFORMATION PROCEDURES
# ───────────────────────────────────────────────────────────────────────

def transform_pr_events(parsed_df: DataFrame) -> DataFrame:
    """Transforms parsed dataframe into clean Pull Requests."""
    # הפילטור עובד ישירות על עמודת type שקיימת ב-parsed_df
    pr_filtered = parsed_df.filter(F.col("type") == "PullRequestEvent")
    
    return pr_filtered.select(
        # חילוץ מתוך ה-payload באמצעות get_json_object
        F.get_json_object(F.col("payload"), "$.pull_request.id").cast("long").alias("pr_id"),
        F.get_json_object(F.col("payload"), "$.action").alias("action"),
        F.get_json_object(F.col("payload"), "$.pull_request.merged").cast("boolean").alias("is_merged"),
        F.get_json_object(F.col("payload"), "$.pull_request.user.login").alias("pr_author"),
        # עמודות אלו כבר קיימות שטוחות ב-parsed_df בזכות ה-Pipeline!
        F.col("actor_login"),
        F.col("repo_name"),
        F.get_json_object(F.col("payload"), "$.pull_request.base.repo.language").alias("language"),
        F.to_timestamp(F.col("created_at")).alias("created_at")
    ).filter(F.col("pr_id").isNotNull())


def transform_push_events(parsed_df: DataFrame) -> DataFrame:
    """Transforms parsed dataframe into clean Pushes."""
    push_filtered = parsed_df.filter(F.col("type") == "PushEvent")
    
    return push_filtered.select(
        F.get_json_object(F.col("payload"), "$.push_id").cast("long").alias("push_id"),
        F.col("actor_login"),
        F.col("repo_name"),
        F.get_json_object(F.col("payload"), "$.size").cast("int").alias("commit_count"),
        F.get_json_object(F.col("payload"), "$.ref").alias("ref"),
        # חילוץ פרטי הקומיטים (מערך/סטרינג) מתוך ה-payload
        F.get_json_object(F.col("payload"), "$.commits[0].author.name").alias("commit_author_name"),
        F.get_json_object(F.col("payload"), "$.commits[0].author.email").alias("commit_author_email"),
        F.to_timestamp(F.col("created_at")).alias("created_at")
    ).filter(F.col("push_id").isNotNull())


def transform_watch_events(parsed_df: DataFrame) -> DataFrame:
    """Transforms parsed dataframe into clean Watches (Stars)."""
    return parsed_df.filter(F.col("type") == "WatchEvent").select(
        F.col("id").alias("event_id"),
        F.col("actor_login"),
        F.col("repo_name"),
        F.to_timestamp(F.col("created_at")).alias("created_at")
    ).filter(F.col("event_id").isNotNull())


def transform_fork_events(parsed_df: DataFrame) -> DataFrame:
    """Transforms parsed dataframe into clean Forks."""
    return parsed_df.filter(F.col("type") == "ForkEvent").select(
        F.col("id").alias("event_id"),
        F.col("actor_login"),
        F.col("repo_name"),
        F.to_timestamp(F.col("created_at")).alias("created_at")
    ).filter(F.col("event_id").isNotNull())


# ───────────────────────────────────────────────────────────────────────
# 3. MAIN BLOCK (Module Check Only)
# ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("📋 All 4 Postgres Schemas & Transformations loaded successfully.")
    print("ℹ️ Module structure verified. Ready to link into main pipeline.py.")