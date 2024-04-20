import warnings
import contextlib

from prefect import flow
from pyspark.sql import SparkSession
from FlightRadar24 import FlightRadar24API
from minio import Minio

from util.config_handler import ConfigHandler
import extract, transform, load
from pyspark.sql import functions as F

fr_api = FlightRadar24API()
config = ConfigHandler("config/config.ini")

# Get the Minio configuration
minio_endpoint = config.get_value("MINIO", "MINIO_ENDPOINT")
minio_access_key = config.get_value("MINIO", "MINIO_ACCESS")
minio_secret_key = config.get_value("MINIO", "MINIO_SECRET")
minio_bucket = config.get_value("MINIO", "MINIO_BUCKET")

# Get the Spark configuration
spark_master_url = config.get_value("SPARK", "SPARK_MASTER_URL")

warnings.filterwarnings("ignore", category=UserWarning)


def get_or_create_df(
    client: Minio, spark: SparkSession, path: str, extract_func, transform_func
):
    """
    Get the DataFrame from the CSV file if it exists, otherwise create the DataFrame from the API data.

    :param client: Minio client
    :param spark: SparkSession
    :param path: path to the CSV file
    :param extract_func: function
    :param transform_func: function
    :return: DataFrame
    """
    # List all objects in the bucket
    objects = client.list_objects(minio_bucket, prefix=path, recursive=True)

    # Check if the CSV file exists
    if not any([obj.object_name == f"{path}/_SUCCESS" for obj in objects]):
        data = extract_func(fr_api)
        df = transform_func(data, spark)
        load.save_df_to_csv(df, f"s3a://{minio_bucket}/{path}")
        print(f"{path} saved to CSV")
    else:
        # Read the CSV file
        print(f"Reading {path}")
        df = spark.read.option("header", "true").csv(f"s3a://{minio_bucket}/{path}")
    return df


def ensure_bucket_exists(client: Minio):
    """
    Ensure that the Minio bucket exists, otherwise create it.

    :param client: Minio client
    """
    if not client.bucket_exists(minio_bucket):
        client.make_bucket("exalt")
        print("Bucket created")


@contextlib.contextmanager
def get_spark_session():
    """
    Create a SparkSession and yield it.
    """
    spark = (
        SparkSession.builder.appName("FlightRadarApp")
        .master(spark_master_url)
        .config("spark.hadoop.fs.s3a.path.style.access", True)
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
        .config(
            "spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory",
        )
        .config("fs.s3a.committer.name", "magic")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,",
        )
        .getOrCreate()
    )

    # Set logging level
    spark.sparkContext.setLogLevel("ERROR")

    try:
        yield spark
    finally:
        spark.stop()


@flow(log_prints=True)
def pipeline_flow():
    config = ConfigHandler("config/config.ini")

    airlines_path = config.get_value("path", "airlines_csv_path")
    airports_path = config.get_value("path", "airports_csv_path")
    flights_path = config.get_value("path", "flights_parquet_path")

    client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False,
    )

    with get_spark_session() as spark:
        ensure_bucket_exists(client)

        airlines_df = get_or_create_df(
            client,
            spark,
            airlines_path,
            extract.get_all_airlines,
            transform.create_airlines_df,
        )
        airports_df = get_or_create_df(
            client,
            spark,
            airports_path,
            extract.get_all_airports,
            transform.create_airports_df,
        )

        flights = extract.get_all_flights(fr_api)
        flights_raw_df = extract.create_flights_raw_df(flights, spark)
        load.save_flights_bronze_csv(
            flights_raw_df, f"s3a://{minio_bucket}/{flights_path}/bronze"
        )
        flights_df = transform.create_flights_silver_df(flights, spark)
        load.save_flights_to_parquet(
            flights_df, f"s3a://{minio_bucket}/{flights_path}/silver"
        )
        flights_df_enriched = transform.flights_enriched_df(
            flights_df, airports_df, airlines_df
        )
        load.save_flights_to_parquet(
            flights_df_enriched, f"s3a://{minio_bucket}/{flights_path}/gold"
        )


if __name__ == "__main__":
    pipeline_flow.serve("pipeline_flow_deployment", interval=3600)
