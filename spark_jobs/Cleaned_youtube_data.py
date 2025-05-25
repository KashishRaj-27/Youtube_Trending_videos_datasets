import json
import os
import logging
import traceback
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, to_date,concat,date_format,regexp_replace
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
)
import psycopg2
import sys, pyspark

# Set Python environment paths early
#os.environ["PYSPARK_PYTHON"] = r"C:\"
#os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\"
#os.environ["JAVA_HOME"] = r"C:\"

print(sys.version)
print(pyspark.__version__)

# Generate timestamped log file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_dir = "C:/path"
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, f"youtube_data_cleaning_{timestamp}.log")

# Setup logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)

CSV_SCHEMA = StructType([
    StructField("video_id", StringType(), nullable=False),
    StructField("trending_date", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("channel_title", StringType(), nullable=True),
    StructField("category_id", IntegerType(), nullable=True),
    StructField("publish_time", TimestampType(), nullable=False),
    StructField("tags", StringType(), nullable=True),
    StructField("views", IntegerType(), nullable=True),
    StructField("likes", IntegerType(), nullable=True),
    StructField("dislikes", IntegerType(), nullable=True),
    StructField("comment_count", IntegerType(), nullable=True),
    StructField("thumbnail_link", StringType(), nullable=True),
    StructField("comments_disabled", BooleanType(), nullable=True),
    StructField("ratings_disabled", BooleanType(), nullable=True),
    StructField("video_error_or_removed", BooleanType(), nullable=True),
    StructField("description", StringType(), nullable=True)
])

CATEGORY_MAPPING = {
    1: 'Film & Animation', 2: 'Autos & Vehicles', 10: 'Music', 15: 'Pets & Animals',
    17: 'Sports', 18: 'Short Movies', 19: 'Travel & Events', 20: 'Gaming',
    21: 'Videoblogging', 22: 'People & Blogs', 23: 'Comedy', 24: 'Entertainment',
    25: 'News & Politics', 26: 'How to Style', 27: 'Education',
    28: 'Science & Technology', 29: 'Nonprofits & Activism', 30: 'Movies',
    31: 'Anime/Animation', 32: 'Action/Adventure', 33: 'Classics',
    34: 'Comedy', 35: 'Documentary', 36: 'Drama', 37: 'Family',
    38: 'Foreign', 39: 'Horror', 40: 'Sci-Fi/Fantasy',
    41: 'Thriller', 42: 'Shorts', 43: 'Shows', 44: 'Trailers'
}

def load_config(path="C:/config_template.json"):
    with open(path, "r") as file:
        return json.load(file)

def create_category_df(spark):
    categories = [(k, v) for k, v in CATEGORY_MAPPING.items()]
    return spark.createDataFrame(categories, schema=["category_id", "category"])

def validate_schema(df):
    expected_fields = {f.name: f.dataType for f in CSV_SCHEMA.fields}
    df_fields = {f.name: f.dataType for f in df.schema.fields}
    for col_name, col_type in expected_fields.items():
        if col_name not in df_fields:
            raise ValueError(f"Missing required column: {col_name}")
        if type(df_fields[col_name]) != type(col_type):
            raise TypeError(f"Column '{col_name}' has type {df_fields[col_name]}, expected {col_type}")
    return True

def get_postgres_table_schema(config, table_name):
    logging.info(f"Fetching PostgreSQL schema for table: {table_name}")
    try:
        conn = psycopg2.connect(
            dbname=config["jdbc_dbname"],
            user=config["jdbc_user"],
            password=config["jdbc_password"],
            host=config["jdbc_host"],
            port=config["jdbc_port"]
        )
        cursor = conn.cursor()
        query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
        """
        cursor.execute(query, (table_name,))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        schema = {}
        for name, dtype, is_nullable in rows:
            schema[name] = {
                "data_type": dtype,
                "is_nullable": is_nullable == 'YES'
            }

        logging.info(f"Retrieved {len(schema)} columns from PostgreSQL schema for table: {table_name}")
        return schema

    except Exception as e:
        logging.error(f"Error fetching schema from PostgreSQL: {e}")
        raise

def check_nulls_in_not_null_columns(df, postgres_schema):
    logging.info("Checking for nulls in NOT NULL PostgreSQL columns...")
    problematic_columns = []

    for col_name, metadata in postgres_schema.items():
        if not metadata["is_nullable"] and col_name in df.columns:
            try:
                null_count = df.filter(col(col_name).isNull()).count()
                if null_count > 0:
                    logging.warning(f"Found {null_count} nulls in NOT NULL column: {col_name}")
                    problematic_columns.append((col_name, null_count))
            except Exception as e:
                logging.error(f"Error checking nulls in column '{col_name}': {e}")
                raise

    if not problematic_columns:
        logging.info("No null violations found in NOT NULL columns.")
    return problematic_columns

def delete_existing_records(config, country):
    try:
        conn = psycopg2.connect(
            dbname=config["jdbc_dbname"],
            user=config["jdbc_user"],
            password=config["jdbc_password"],
            host=config["jdbc_host"],
            port=config["jdbc_port"]
        )
        cursor = conn.cursor()
        delete_query = f"DELETE FROM {config['jdbc_table']} WHERE country = %s"
        cursor.execute(delete_query, (country,))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Deleted existing records for country: {country}")
    except Exception as e:
        logging.error(f"Error deleting records for {country}: {e}")
        raise

def clean_and_save_dataset(input_path, jdbc_url, jdbc_table, jdbc_user, jdbc_password, config, country, spark, max_retries=1):
    attempt = 0
    parquet_path = os.path.join(config['parquet_root_path'], f"{country}_cleaned.parquet")

    while attempt < max_retries:
        try:
            logging.info(f"Reading CSV for country '{country}' from {input_path}")
            df = spark.read.option("header", True).schema(CSV_SCHEMA).csv(input_path)

            validate_schema(df)
            logging.info("Schema validation passed")

            df = df.withColumn("publish_time", regexp_replace("publish_time", "Z$", ""))
            df = df.withColumn("publish_time", to_timestamp(col("publish_time")))

            # Convert trending_date 'yy.dd.MM' to date
            df = df.withColumn("trending_date", to_date("trending_date", "yy.dd.MM"))

            # Convert trending_date to timestamp at midnight (start of day)
            df = df.withColumn("trending_date", to_timestamp(col("trending_date")))

            # Optional: Format both as string with same pattern if needed
            # Example format: 'yyyy-MM-dd HH:mm:ss'
            df = df.withColumn("publish_time_str", date_format(col("publish_time"), "yyyy-MM-dd HH:mm:ss"))
            df = df.withColumn("trending_date_str", date_format(col("trending_date"), "yyyy-MM-dd HH:mm:ss"))

            # Drop before writing to database
            df = df.drop("publish_time_str", "trending_date_str")
            
            df = df.filter(col("publish_time").isNotNull() & col("video_id").isNotNull())

            

            df = df.withColumn("country", lit(country))
            df = df.join(create_category_df(spark), on="category_id", how="left")
            df = df.dropDuplicates(["video_id", "country"])

            postgres_schema = get_postgres_table_schema(config, jdbc_table)
            null_violations = check_nulls_in_not_null_columns(df, postgres_schema)
            if null_violations:
                for col_name, null_count in null_violations:
                    logging.error(f"Column '{col_name}' has {null_count} nulls but is NOT NULL in the DB.")
                raise ValueError("Null violations detected in NOT NULL DB columns.")

            df = df.filter(
                (~col("video_id").isin("#NAME?")) &
                col("video_id").isNotNull() &
                col("publish_time").isNotNull() &
                col("country").isNotNull()
            )

            delete_existing_records(config, country)

            df.write.mode("overwrite").parquet(parquet_path)

            logging.info(f"Writing cleaned data to PostgreSQL for country '{country}'")
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", jdbc_table) \
                .option("user", jdbc_user) \
                .option("password", jdbc_password) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", "1000") \
                .mode("append") \
                .save()

            logging.info(f"Successfully processed and saved dataset for {country}")
            break
        except Exception as e:
            attempt += 1
            logging.error(f"--- Attempt {attempt} failed for {country}: {e} ---")
            logging.error(traceback.format_exc())
            if attempt >= max_retries:
                logging.error(f"Max retries reached for {country}. Stopping process.")
                raise
            else:
                wait_sec = 5 * attempt
                logging.info(f"Retrying after {wait_sec} seconds...")
                time.sleep(wait_sec)

def process_all_datasets(config, spark):
    jdbc_url = f"jdbc:postgresql://{config['jdbc_host']}:{config['jdbc_port']}/{config['jdbc_dbname']}"
    for dataset in config["datasets"]:
        input_path = dataset["input"]
        country = dataset["country"]
        logging.info(f"Starting processing for dataset {input_path} (Country: {country})")

        try:
            clean_and_save_dataset(
                input_path=input_path,
                jdbc_url=jdbc_url,
                jdbc_table=config["jdbc_table"],
                jdbc_user=config["jdbc_user"],
                jdbc_password=config["jdbc_password"],
                country=country,
                config=config,
                spark=spark,
                max_retries=1
            )
        except Exception as e:
            logging.error(f"Failed processing dataset {input_path} for country {country}: {e}")
            break
        logging.info(f"Finished processing dataset for country {country}")

def main():
    config = load_config()
    spark = SparkSession.builder \
        .appName("YouTube Data Cleaning") \
        .master(config.get("spark_master", "local[*]")) \
        .config("spark.executor.memory", config.get("spark_executor_memory", "2g")) \
        .config("spark.driver.memory", config.get("spark_driver_memory", "2g")) \
        .config("spark.jars", r"C:\jar_path") \
        .getOrCreate()

    try:
        process_all_datasets(config, spark)
        logging.info("All datasets processed successfully.")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user.")
    except Exception as e:
        logging.error(f"Processing stopped: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
