from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder \
        .appName("PeekBronzeData") \
        .master("local[1]") \
        .getOrCreate()

    try:
        df = spark.read.parquet("/data/bronze/events")
        print("\n" + "="*60)
        print(f"STATUS: Total Raw Records in Bronze Folder: {df.count()}")
        print("="*60)
        
        df.printSchema()
        
        # המרת ה-Value מבינארי לטקסט כדי שנוכל לראות את ה-JSON
        df_readable = df.withColumn("value_str", col("value").cast("string"))
        
        print("\nSAMPLE DATA FROM KAFKA VALUE (Top 2):")
        df_readable.select("partition", "offset", "timestamp", "value_str").show(2, truncate=100)
        print("="*60 + "\n")
    except Exception as e:
        print(f"\n[ERROR] Could not read parquet (Maybe no files written yet?): {e}\n")

if __name__ == "__main__":
    main()