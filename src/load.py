from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def save_airlines_to_csv(airlines_df: DataFrame, spark: SparkSession):
    airlines_df.write.option("header", "true").csv("../data/Airlines.csv")
    print("Airlines saved to CSV")

def save_airports_to_csv(airports_df: DataFrame, spark: SparkSession):
    airports_df.write.option("header", "true").csv("../data/Airports.csv")
    print("Airports saved to CSV")

def save_flights_to_csv(flights_df: DataFrame, spark: SparkSession):
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
    date_path = f"year={year}/month={month}/day={day}/{file_name}"
    path = f"../data/Flights/rawzone/{date_path}"

    # path = f"s3a://{MINIO_BUCKET}/Flights/rawzone/{date_path}"

    # Make the Minio bucket if it doesn't exist
    # if not client.bucket_exists("exalt"):
    #     client.make_bucket("exalt")
    #     print("Bucket created")
    
    # Save the DataFrame to a CSV file
    flights_df.write.option("header", "true").csv(path)

    print(f"Flights saved to {path}")
    
    spark.stop()