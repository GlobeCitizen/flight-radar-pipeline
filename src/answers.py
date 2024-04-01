import glob
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, first
from pyspark.sql.types import FloatType

def create_spark_session():
    spark_master_url = "local[*]"
    spark = SparkSession.builder \
        .appName("FlightRadarApp") \
        .master(spark_master_url) \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .getOrCreate()
    return spark

def get_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points given their latitude and longitude in kilometers.

    :param lat1: float
    :param lon1: float
    :param lat2: float
    :param lon2: float
    :return: float
    """
    # Check if the values are float
    if not all(map(lambda x: isinstance(x, float), [lat1, lon1, lat2, lon2])):
        return None
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(
        lambda x: x * (3.14159265359 / 180),
        [lat1, lon1, lat2, lon2]
    )

    # Calculate the distance using the haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (pow((pow((pow((pow(math.sin(dlat / 2), 2)), 2) + pow(math.sin(dlon / 2), 2)), 2)), 0.5))
    c = 2 * math.asin(a)
    r = 6371
    return c * r

def flights_with_airports(flights_df, airports_df):
    """
    Join the flights_df and airports_df DataFrames to get the latitude, longitude, and continent of the origin and destination airports.

    :param flights_df: spark DataFrame
    :param airports_df: spark DataFrame
    :return: spark DataFrame
    """

    flight_airport_df = flights_df.join(
        airports_df.alias("origin_airport"),
        flights_df.origin_airport_iata == airports_df.iata
    ).select(
        flights_df["*"],
        col("origin_airport.latitude").alias("origin_latitude"),
        col("origin_airport.longitude").alias("origin_longitude"),
        col("origin_airport.continent").alias("origin_continent")
    )

    flight_airport_df = flight_airport_df.join(
        airports_df.alias("destination_airport"),
        flights_df.destination_airport_iata == airports_df.iata
    ).select(
        flight_airport_df["*"],
        col("destination_airport.latitude").alias("destination_latitude"),
        col("destination_airport.longitude").alias("destination_longitude"),
        col("destination_airport.continent").alias("destination_continent")
    )

    return flight_airport_df

def get_airline_with_most_flights(flights_df, airlines_df):
    """
    Get the airline with the most flights.

    :param flights_df: spark DataFrame
    :param airlines_df: spark DataFrame
    :return: str
    """
    # Join the flights_df and airlines_df DataFrames
    joined_df = flights_df.join(airlines_df, flights_df.airline_icao == airlines_df.ICAO, "inner")
    # Group by the airline name and count the number of flights
    airline_flights = joined_df.groupBy("Name").count()

    # Get the airline with the most flights and the number of flights
    airline_with_most_flights = airline_flights.orderBy("count", ascending=False).first()

    return airline_with_most_flights

def most_active_airline_per_continent(flights_df, airlines_df, airports_df):
    """
    Get the most active airline per continent.

    :param flights_df: spark DataFrame
    :param airlines_df: spark DataFrame
    :param airports_df: spark DataFrame
    :return: dict
    """

    # Join the flights_df and airports_df DataFrames
    flights_df_airport_continent = flights_with_airports(flights_df, airports_df)   

    # Join the flights_df and airlines_df DataFrames
    flights_df_airlines = flights_df_airport_continent.join(airlines_df, flights_df.airline_icao == airlines_df.ICAO, "inner").select(flights_df_airport_continent["*"], airlines_df.Name.alias("airline_name"))

    # Group by the airline name and continent and count the number of flights where the origin and destination continents are the same
    regional_airline_flights = flights_df_airlines.filter(flights_df_airlines.origin_continent == flights_df_airlines.destination_continent).groupBy("airline_name", "origin_continent").count()

    # Get the most active airline per continent
    most_active_airline_per_continent = regional_airline_flights.orderBy("count", ascending=False).groupBy("origin_continent").agg(
        first("airline_name").alias("most_active_airline")
    ).collect()

    return {row["origin_continent"]: row["most_active_airline"] for row in most_active_airline_per_continent}

def flight_with_longest_trajectory(flights_df, airports_df):
    """
    Get the flight with the longest trajectory.

    :param flights_df: spark DataFrame
    :param airports_df: spark DataFrame
    :return: dict
    """
    # Join the flights_df and airports_df DataFrames
    flights_df_airports = flights_with_airports(flights_df, airports_df)
    
    # define a distance udf
    get_distance_udf = udf(get_distance, FloatType())

    # Calculate the distance between the origin and destination airports
    flights_df_airports = flights_df_airports.withColumn(
        "distance",
        get_distance_udf(
            col("origin_latitude"),
            col("origin_longitude"),
            col("destination_latitude"),
            col("destination_longitude")
        )
    )

    # Get the flight with the longest trajectory
    longest_trajectory_flight = flights_df_airports.orderBy("distance", ascending=False).first()

    return longest_trajectory_flight

if __name__ == "__main__":
    spark = create_spark_session()

    # Load the Airlines DataFrame
    airlines_df = spark.read.option("header", "true").csv("../data/Airlines.csv")

    #Load the Airports DataFrame
    airports_df = spark.read.option("header", "true").csv("../data/Airports.csv")

    # Load the most recent Flights DataFrame
    files = glob.glob("../data/Flights/rawzone/*/*/*/*.csv")
    files.sort()
    most_recent_file = files[-1]

    flights_df = spark.read.option("header", "true").csv(most_recent_file)

    # Get the airline with the most flights
    airline_with_most_flights = get_airline_with_most_flights(flights_df, airlines_df)

    print(f"The airline with the most flights is: {airline_with_most_flights.Name} with {airline_with_most_flights["count"]} flights")

    # Get the most active airline per continent
    most_active_airline_per_continent = most_active_airline_per_continent(flights_df, airlines_df, airports_df)
    print("The most active airline per continent is:")
    for continent, airline in most_active_airline_per_continent.items():
        print(f"{continent}: {airline}")

    # Get the flight with the longest trajectory
    longest_trajectory_flight = flight_with_longest_trajectory(flights_df, airports_df)
    print("The flight with the longest trajectory is:")
    print(longest_trajectory_flight)

    spark.stop()
