import os, json, warnings
from pyspark.sql import SparkSession
from FlightRadar24 import FlightRadar24API
from minio import Minio

import extract, transform, load


fr_api = FlightRadar24API()

MINIO_BUCKET = "exalt"

client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

warnings.filterwarnings("ignore", category=UserWarning)

def create_spark_session():
    spark_master_url = "local[*]"
    spark = SparkSession.builder \
        .appName("FlightRadarApp") \
        .master(spark_master_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .getOrCreate()
        # .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
        # .config("spark.hadoop.fs.s3a.secret.key",os.getenv("MINIO_SECRET_KEY")) \
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        # .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0") \
    return spark


def pipeline_flow():
    with open(os.path.join(os.pardir, 'config.json'), 'r') as f:
        config = json.load(f)
    
    airlines_path = config['airlines_csv_path']
    airports_path = config['airports_csv_path']
    flights_path = config['flights_csv_path']

    spark = create_spark_session()
    if os.path.exists(airlines_path):
        airlines_df = spark.read.option("header", "true").csv(airlines_path)
    else:
        airlines = extract.get_all_airlines(fr_api)
        airlines_df = transform.create_airlines_df(airlines, spark)
        load.save_df_to_csv(airlines_df, spark, airlines_path)
    
    if os.path.exists(airports_path):
        airports_df = spark.read.option("header", "true").csv(airports_path)
    else:
        airports = extract.get_all_airports(fr_api)
        airports_df = transform.create_airports_df(airports, spark)
        load.save_df_to_csv(airports_df, spark, airports_path)

    flights = extract.get_all_flights(fr_api)
    flights_df = transform.create_flights_df(flights, spark)
    flights_df_enriched = transform.flights_enriched_df(flights_df, airports_df, airlines_df, spark)
    load.save_flights_to_parquet(flights_df_enriched, spark, flights_path)

    spark.stop()

if __name__ == "__main__":
    pipeline_flow()