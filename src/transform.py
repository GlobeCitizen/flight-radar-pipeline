import math

from prefect import task
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import requests

import pycountry_convert as pc

# def get_manufacturer(icao24: str) -> str:
#     """
#     Get the manufacturer of an aircraft given its icao24 code.

#     :param icao24: str
#     :return: str
#     """
#     url = f"https://aviation-reference-data.p.rapidapi.com/icao24/{icao24}"

#     headers = {
#         "X-RapidAPI-Key": "795430c3f0mshc5c7b6c25258cdap13cfa0jsn1b11f568db32",
#         "X-RapidAPI-Host": "aviation-reference-data.p.rapidapi.com"
#     }

#     response = requests.get(url, headers=headers)
#     data = response.json()

#     if "manufacturer" in data:
#         return data["manufacturer"]
#     else:
#         return None

@F.udf(returnType=StringType())
def get_continent(country: str) -> str:
    try:
        # Convert the country name to country code
        country_code = pc.country_name_to_country_alpha2(country, cn_name_format="default")
        # Convert the country code to continent code
        continent_code = pc.country_alpha2_to_continent_code(country_code)
        # Convert the continent code to continent name
        continent_name = pc.convert_continent_code_to_continent_name(continent_code)
        return continent_name
    except Exception:
        return None


@F.udf(returnType=FloatType())
def get_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points given their latitude and longitude in kilometers.

    :param lat1: float
    :param lon1: float
    :param lat2: float
    :param lon2: float
    :return: float
    """

    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    
    # cast all values to float if it's possible
    lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(
        lambda x: math.radians(x),
        [lat1, lon1, lat2, lon2]
    )

    # Calculate the distance using the haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371
    return c * r
    
# @task()
def create_airlines_df(airlines: list, spark: SparkSession) -> DataFrame:
    """
    Create a DataFrame from the list of airlines.

    :param airlines: list
    :param spark: SparkSession
    :return: DataFrame
    """
    # Define the schema for the airlines DataFrame
    airline_schema = StructType([
        StructField("Name", StringType(), True),
        StructField("ICAO", StringType(), True),
    ])

    # Convert the list of airlines to a DataFrame

    airlines_df = spark.createDataFrame(airlines, schema=airline_schema)
    print(f"{airlines_df.count()} airlines found")

    return airlines_df


# @task()
def create_airports_df(airports: list, spark: SparkSession) -> DataFrame:
    """
    Create a DataFrame from the list of airports.
    
    :param airports: list
    :param spark: SparkSession
    :return: DataFrame
    """

    # Define the schema for the airports DataFrame
    airport_schema = StructType([
        StructField("iata", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("country", StringType(), True),
    ])

    airports_data = [
        (
        airport.iata,
        float(airport.latitude),
        float(airport.longitude),
        airport.country
        )
        for airport in airports
    ]

    airports_df = spark.createDataFrame(airports_data, schema=airport_schema)
    print(f"{airports_df.count()} airports found")


    # Add the continent column 
    airports_df = airports_df.withColumn("continent", get_continent(airports_df["country"]))

    return airports_df


# @task()
def create_flights_df(flights: list, spark: SparkSession) -> DataFrame:
    """
    Create a DataFrame from the list of flights.

    :param flights: list
    :param spark: SparkSession
    :return: DataFrame
    """
    #define a data schema for pySpark df
    flight_schema = StructType([
        StructField("id", StringType(), True),
        StructField("aircraft_code", StringType(), True),
        StructField("time", IntegerType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("origin_airport_iata", StringType(), True),
        StructField("destination_airport_iata", StringType(), True),
        StructField("number", StringType(), True),
        StructField("on_ground", IntegerType(), True),
        StructField("airline_icao", StringType(), True)
    ])


    # Convert the list of flights to a DataFrame
    flights_df = spark.createDataFrame(flights, schema=flight_schema)
    print(f"{flights_df.count()} flights found")

    # Remove duplicates
    flights_df = flights_df.dropDuplicates(["id"])

    print(f"{flights_df.count()} flights after removing duplicates")
    # flights_df = flights_df.withColumn("time", from_unixtime("time", "yyyy-MM-dd HH:mm:ss"))

    return flights_df

# @task()
def flights_enriched_df(flights_df: DataFrame, airports_df: DataFrame, airlines: DataFrame) -> DataFrame:
    """
    Enrich the flights DataFrame with the flight distance, airport infos and airlines names.

    :param flights_df: spark DataFrame
    :param airports_df: spark DataFrame
    :param airlines: spark DataFrame
    :param spark: SparkSession
    :return: DataFrame
    """
    # Join the flights_df and airports_df DataFrames
    flight_airport_df = flights_df.join(
        airports_df.alias("origin_airport"),
        flights_df.origin_airport_iata == airports_df.iata
    ).select(
        flights_df["*"],
        F.col("origin_airport.latitude").alias("origin_latitude").cast(FloatType()),
        F.col("origin_airport.longitude").alias("origin_longitude").cast(FloatType()),
        F.col("origin_airport.continent").alias("origin_continent"),
        F.col("origin_airport.country").alias("origin_country")
    )

    flight_airport_df = flight_airport_df.join(
        airports_df.alias("destination_airport"),
        flights_df.destination_airport_iata == airports_df.iata
    ).select(
        flight_airport_df["*"],
        F.col("destination_airport.latitude").alias("destination_latitude").cast(FloatType()),
        F.col("destination_airport.longitude").alias("destination_longitude").cast(FloatType()),
        F.col("destination_airport.continent").alias("destination_continent"),
        F.col("destination_airport.country").alias("destination_country")
    )

    # Join the flights_df and airlines_df DataFrames
    flights_df_airports_airlines = flight_airport_df \
        .join(airlines, flight_airport_df.airline_icao == airlines.ICAO) \
        .select(flight_airport_df["*"], airlines.Name.alias("airline_name"))

    #define a get_manufacturer udf
    # get_manufacturer_udf = F.udf(get_manufacturer, StringType())

    # Calculate the distance between the origin and destination airports
    flights_df_airports_airlines = flights_df_airports_airlines.withColumn(
        "distance",
        get_distance(
            F.col("origin_latitude"),
            F.col("origin_longitude"),
            F.col("destination_latitude"),
            F.col("destination_longitude")
        )
    )

    return flights_df_airports_airlines

