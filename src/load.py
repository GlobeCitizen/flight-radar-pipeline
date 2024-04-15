from datetime import datetime
import pytz
from minio import Minio
from prefect import task
from pyspark.sql import DataFrame

# @task()
def save_df_to_csv(df: DataFrame, path: str):
    """
    Save the dataFrame to a CSV file.

    :param df: spark DataFrame
    :param spark: SparkSession
    :param path: csv file path
    """
    print(f"Saving DataFrame to {path}")
    df.coalesce(1).write.option("header", "true").mode("overwrite").format("csv").save(path)

# @task()
def save_flights_to_parquet(flights_df: DataFrame, path: str, client: Minio):
    """
    Save the flights_df data to a Parquet file.

    :param flights_df: spark DataFrame
    :param path: csv file path
    :param client: Minio client
    """

    # Get the current date and time
    now = datetime.now(pytz.timezone("Europe/Paris"))

    # Format the date and time as per the required format
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time = now.strftime("%H%M%S")

    # Create the file name
    file_name = f"flights{year+month+day+time}.parquet"

    # Create the path to the Minio bucket
    date = f"year={year}/month={month}/day={day}/{file_name}"
    file_path = path + date
    path = f"s3a://exalt/{file_path}"
    
    # Save the DataFrame to a CSV file
    flights_df.write.format("parquet").mode("overwrite").save(path)

    print(f"Flights saved to {file_path}")