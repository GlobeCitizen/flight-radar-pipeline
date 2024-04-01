from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime

import pycountry_convert as pc

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

def create_airports_df(airports: list, spark: SparkSession) -> DataFrame:
    airport_schema = StructType([
        StructField("iata", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("country", StringType(), True),
    ])

    airports_data = [
        (airport.iata,
        float(airport.latitude),
        float(airport.longitude),
        airport.country
        )
        for airport in airports
    ]

    airports_df = spark.createDataFrame(airports_data, schema=airport_schema)
    print(f"{airports_df.count()} airports found")

    # Define a UDF that gets the continent for a given country
    get_continent_udf = udf(get_continent, StringType())

    # Add the continent column
    airports_df = airports_df.withColumn("continent", get_continent_udf(airports_df["country"]))

    return airports_df

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
        StructField("icao_24bit", StringType(), True),
        StructField("aircraft_code", StringType(), True),
        StructField("time", DoubleType(), True),
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

