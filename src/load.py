from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def save_df_to_csv(df: DataFrame, spark: SparkSession, path: str):
    """
    Save the dataFrame to a CSV file.

    :param df: spark DataFrame
    :param spark: SparkSession
    :param path: csv file path
    """
    df.write.option("header", "true").csv(path)
    print("Airlines saved to CSV")

def save_flights_to_csv(flights_df: DataFrame, spark: SparkSession, path: str):
    """
    Save the flights_df data to a CSV file.

    :param flights_df: spark DataFrame
    :param spark: SparkSession
    """

    # Get the current date and time
    now = datetime.now()

    # Format the date and time as per the required format
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    time = now.strftime("%H%M%S")

    # Create the file name
    file_name = f"flights{year+month+day+time}.csv"

    # Create the path to the Minio bucket
    date = f"year={year}/month={month}/day={day}/{file_name}"
    file_path = path + date
    # path = f"s3a://{MINIO_BUCKET}/Flights/rawzone/{date_path}"

    # Make the Minio bucket if it doesn't exist
    # if not client.bucket_exists("exalt"):
    #     client.make_bucket("exalt")
    #     print("Bucket created")
    
    # Save the DataFrame to a CSV file
    flights_df.write.option("header", "true").csv(file_path)

    print(f"Flights saved to {file_path}")