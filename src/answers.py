import glob
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import typer
from util.config_handler import ConfigHandler

app = typer.Typer()

def create_spark_session():
    spark_master_url = "local[*]"
    spark = SparkSession.builder \
        .appName("FlightRadarApp") \
        .master(spark_master_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .getOrCreate()
    return spark

def get_airline_with_most_flights(flights_df):
    """
    Get the airline with the most flights.

    :param flights_df: spark DataFrame
    :param airlines_df: spark DataFrame
    :return: str
    """

    # Group by the airline name and count the number of flights
    airline_flights = flights_df.groupBy("airline_name").count()

    # Get the airline with the most flights and the number of flights
    airline_with_most_flights = airline_flights.orderBy("count", ascending=False).first()

    return airline_with_most_flights


def get_most_active_airline_per_continent(flights_df):
    """
    Get the most active airline per continent.

    :param flights_df: spark DataFrame
    :param airlines_df: spark DataFrame
    :param airports_df: spark DataFrame
    :return: dict
    """

    # Group by the airline name and continent and count the number of flights where the origin and destination continents
    # are the same
    regional_airline_flights = flights_df \
                                .filter(flights_df.origin_continent == flights_df.destination_continent) \
                                .groupBy("airline_name", "origin_continent").count()

    # Get the most active airline per continent
    most_active_airline_per_continent = regional_airline_flights \
                                        .orderBy("count", ascending=False) \
                                        .groupBy("origin_continent") \
                                        .agg(F.first("airline_name").alias("most_active_airline")) \
                                        .collect()

    return {row["origin_continent"]: row["most_active_airline"] for row in most_active_airline_per_continent}


def flight_with_longest_trajectory(flights_df):
    """
    Get the flight with the longest trajectory.

    :param flights_df: spark DataFrame
    :param airports_df: spark DataFrame
    :return: dict
    """

    # Get the flight with the longest trajectory
    longest_trajectory_flight = flights_df.orderBy("distance", ascending=False).first()

    return longest_trajectory_flight


def get_average_flight_length_per_continent(flights_df) -> dict:
    """
    Get the average flight length per continent.

    :param flights_df: spark DataFrame
    :return: dict
    """

    # filter out flights with different origin and destination continents
    regional_flights = flights_df.filter(flights_df.origin_continent == flights_df.destination_continent)

    # Group by the continent and calculate the average distance
    average_flight_length_per_continent = regional_flights.groupBy("origin_continent") \
                                            .agg(F.avg("distance").alias("average_distance")).collect()
    
    return {row["origin_continent"]: round(row["average_distance"], 2) for row in average_flight_length_per_continent}


def get_top_three_aircraft_model_per_country(flights_df) -> dict:
    """
    Get the top three aircraft model per country.

    :param flights_df: spark DataFrame
    :return: dict
    """

    # Group by the aircraft model and country and count the number of flights
    aircraft_model_flights = flights_df.groupBy("aircraft_code", "origin_country").count()

    # Get the top three aircraft model per country
    result = flights_df.groupBy('origin_country', 'aircraft_code') \
                        .count() \
                        .withColumn('rank', F.row_number().over(Window.partitionBy('origin_country').orderBy(F.desc('count')))) \
                        .filter(F.col('rank') <= 3)

    top_three_aircraft_model_per_country = result.groupBy("origin_country") \
                                            .agg(F.concat_ws(", ", F.collect_list(result.aircraft_code)).alias("top_three_aircraft_model")) \
                                            .collect()
    
    return {row["origin_country"]: row["top_three_aircraft_model"] for row in top_three_aircraft_model_per_country}


# Get the airline with the most flights
@app.command()
def airline_with_most_flights():
    airline_with_most_flights = get_airline_with_most_flights(flights_df)

    print(f"The airline with the most flights is: {airline_with_most_flights.airline_name} with {airline_with_most_flights["count"]} flights")

# # Get the most active airline per continent
@app.command()
def most_active_airline_per_continent():
    most_active_airline_per_continent = get_most_active_airline_per_continent(flights_df)
    print("The most active airline per continent is:")
    for continent, airline in most_active_airline_per_continent.items():
        print(f"{continent}: {airline}")

# # Get the flight with the longest trajectory
@app.command()
def longest_trajectory_flight():
    longest_trajectory_flight = flight_with_longest_trajectory(flights_df)
    print("The flight with the longest trajectory is:", longest_trajectory_flight)

# # Get the average flight length per continent
@app.command()
def average_flight_length_per_continent():
    average_flight_length_per_continent = get_average_flight_length_per_continent(flights_df)
    print("The average flight length per continent is:")
    for continent, average_distance in average_flight_length_per_continent.items():
        print(f"{continent}: {average_distance}")

# # Get the top three aircraft model per country
@app.command()
def top_three_aircraft_model_per_country():
    top_three_aircraft_model_per_country = get_top_three_aircraft_model_per_country(flights_df)
    print("The top three aircraft model per country is:")
    for country, aircraft_model in top_three_aircraft_model_per_country.items():
        print(f"{country}: {aircraft_model}")


if __name__ == "__main__":
    config = ConfigHandler("config/config.ini")

    airlines_path = config.get_value("path", "airlines_csv_path")
    airports_path = config.get_value("path", "airports_csv_path")
    flights_path = config.get_value("path", "flights_parquet_path")

    spark = create_spark_session()

    # Load the most recent Flights DataFrame
    files = glob.glob(f"{flights_path}/*/*/*/*.parquet")
    files.sort()
    most_recent_file = files[-1]

    flights_df = spark.read.option("header", "true").parquet(most_recent_file)
    app()
    spark.stop()
