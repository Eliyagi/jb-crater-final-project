import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session() -> SparkSession:
    """מאתחל ומחזיר SparkSession מקומי עבור הציצה לנתונים"""
    return SparkSession.builder \
        .appName("PeekBronzeData") \
        .master("local[1]") \
        .getOrCreate()

def peek_bronze_data(spark: SparkSession, input_path: str):
    """קורא את נתוני הפארקט ומציג מטריקות ודגימת נתונים בצורה בולטת במיוחד"""
    
    # הגדרת קודי צבע לטרמינל (ANSI) בראש הפונקציה - כדי שיהיו זמינים תמיד
    GREEN = '\033[92m'
    CYAN = '\033[96m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    RESET = '\033[0m'

    # סניטי צ'ק - אם התיקייה לא קיימת או ריקה מקבצים, נעצור בנימוס במקום לקרוס
    if not os.path.exists(input_path) or not os.listdir(input_path):
        print("\n" + "!"*80)
        print(f"{YELLOW}{BOLD}[WARNING] BRONZE PATH '{input_path}' IS CURRENTLY EMPTY OR DOES NOT EXIST!{RESET}")
        print(f"{CYAN}Please run the Bronze Pipeline (gh-archive-vendor) first to generate data.{RESET}")
        print("!"*80 + "\n")
        return

    try:
        # קריאת קבצי הפארקט מהברונז
        df = spark.read.parquet(input_path)
        
        # 1. הדפסת כמות הרשומות הכוללת בבאנר ענק
        print("\n" + "="*80)
        print("="*80)
        print(f"{GREEN}{BOLD}██████╗ ██████╗  ██████╗ ███╗   ██╗███████╗███████╗{RESET}")
        print(f"{GREEN}{BOLD}██╔══██╗██╔══██╗██╔═══██╗████╗  ██║╚══███╔╝██╔════╝{RESET}")
        print(f"{GREEN}{BOLD}██████╔╝██████╔╝██║   ██║██╔██╗ ██║  ███╔╝ █████╗  {RESET}")
        print(f"{GREEN}{BOLD}██╔══██╗██╔══██╗██║   ██║██║╚██╗██║ ███╔╝  ██╔══╝  {RESET}")
        print(f"{GREEN}{BOLD}██████╔╝██║  ██║╚██████╔╝██║ ╚████║███████╗███████╗{RESET}")
        print(f"{GREEN}{BOLD}╚══════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚══════╝╚══════╝{RESET}")
        print("="*80)
        print(f"{GREEN}{BOLD} STATUS: TOTAL RAW RECORDS IN BRONZE FOLDER --> {df.count()}{RESET}")
        print("="*80)
        print("="*80 + "\n")
        
        # 2. הדפסת הסכמה הנוכחית של הברונז
        print("="*60)
        print(f"{CYAN}{BOLD}>>>>>> CURRENT BRONZE SCHEMA <<<<<<{RESET}")
        print("="*60)
        df.printSchema()
        print("="*60 + "\n")
        
        # 3. הצגת דגימה של הנתונים האמיתיים
        print("="*60)
        print(f"{YELLOW}{BOLD}>>>>>> SAMPLE DATA FROM BRONZE (TOP 3 RUNS) <<<<<<{RESET}")
        print("="*60)
        
        # בחירת עמודות והצגה עם פורמט רחב ונקי
        df.select(
            col("id").alias("EVENT_ID"), 
            col("type").alias("EVENT_TYPE"), 
            col("created_at").alias("CREATED_AT"), 
            col("payload").alias("RAW_PAYLOAD")
        ).show(3, truncate=90)
        
        print("="*60)
        print("="*60 + "\n")
        
    except Exception as e:
        print("\n" + "!"*60)
        print(f"{RED}{BOLD}[ERROR] COULD NOT READ PARQUET FROM {input_path}{RESET}")
        print(f"{RED}Detail: {e}{RESET}")
        print("!"*60 + "\n")
        sys.exit(1)

def main():
    INPUT_PATH = "/data/bronze/events"
    
    spark = create_spark_session()
    # משתיקים את ה-INFO של ספארק כדי שרק ה-Banners שלנו יודפסו
    spark.sparkContext.setLogLevel("WARN")  
    
    peek_bronze_data(spark, INPUT_PATH)

if __name__ == "__main__":
    main()