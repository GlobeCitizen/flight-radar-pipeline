import os, warnings, sys
from prefect import flow
from pyspark.sql import SparkSession
from FlightRadar24 import FlightRadar24API
from minio import Minio
import contextlib
from util.config_handler import ConfigHandler

import extract, transform, load

fr_api = FlightRadar24API()

MINIO_BUCKET = "exalt"

warnings.filterwarnings("ignore", category=UserWarning)

@contextlib.contextmanager
def get_spark_session():
    config = ConfigHandler('config/config.ini')

    minio_access_key = config.get_value('MINIO', 'MINIO_ACCESS')
    minio_secret_key = config.get_value('MINIO', 'MINIO_SECRET')

    spark_master_url = "spark://localhost:7077"

    spark = SparkSession.builder \
        .appName("FlightRadarApp") \
        .master(spark_master_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.jars.packages",
                 "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.apache.spark:spark-hadoop-cloud_2.12:3.3.1"
                )\
        .getOrCreate()
    
    # spark.sparkContext.addFile("../dist/fligh_radar_pipeline-0.1-py3.11.egg")
    
    try:
        yield spark
    finally:
        spark.stop()

# @flow(log_prints=True)
def pipeline_flow():
    config = ConfigHandler('config/config.ini')
    
    airlines_path = config.get_value('path', 'airlines_csv_path')
    airports_path = config.get_value('path', 'airports_csv_path')
    flights_path = config.get_value('path', 'flights_csv_path')
    minio_access_key = config.get_value('MINIO', 'MINIO_ACCESS')
    minio_secret_key = config.get_value('MINIO', 'MINIO_SECRET')

    client = Minio(
        "localhost:9000",
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )
    
    with get_spark_session() as spark:

        if os.path.exists(airlines_path):
            airlines_df = spark.read.option("header", "true").csv(airlines_path)
        else:
            airlines = extract.get_all_airlines(fr_api)
            airlines_df = transform.create_airlines_df(airlines, spark)
            load.save_df_to_csv(airlines_df, airlines_path, spark, client)

        if os.path.exists(airports_path):
            airports_df = spark.read.option("header", "true").csv(airports_path)
        else:
            airports = extract.get_all_airports(fr_api)
            airports_df = transform.create_airports_df(airports, spark)
            load.save_df_to_csv(airports_df, airports_path, spark, client)

        flights = extract.get_all_flights(fr_api)
        flights_df = transform.create_flights_df(flights, spark)
        flights_df_enriched = transform.flights_enriched_df(flights_df, airports_df, airlines_df)
        flights_df_enriched.show()
        # load.save_flights_to_parquet(flights_df_enriched, flights_path, client)

if __name__ == "__main__":
    pipeline_flow()