from prefect import task
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from util.config_handler import ConfigHandler

config = ConfigHandler("config/config.ini")
API_LIMIT = config.get_value("API", "API_LIMIT")


def divide_zone(zone_data: dict) -> dict:
    """
    Divide a zone into 4 subzones.

    :param zone_data: dict
    :return: dict
    """
    min_lat, max_lat, min_lon, max_lon = (
        zone_data["br_y"],
        zone_data["tl_y"],
        zone_data["tl_x"],
        zone_data["br_x"],
    )
    # Calculate the middle point of the zone
    mid_lat, mid_lon = (min_lat + max_lat) / 2, (min_lon + max_lon) / 2
    return {
        "top_left": {
            "tl_y": max_lat,
            "tl_x": min_lon,
            "br_y": mid_lat,
            "br_x": mid_lon,
        },
        "top_right": {
            "tl_y": max_lat,
            "tl_x": mid_lon,
            "br_y": mid_lat,
            "br_x": max_lon,
        },
        "bottom_left": {
            "tl_y": mid_lat,
            "tl_x": min_lon,
            "br_y": min_lat,
            "br_x": mid_lon,
        },
        "bottom_right": {
            "tl_y": mid_lat,
            "tl_x": mid_lon,
            "br_y": min_lat,
            "br_x": max_lon,
        },
    }


def get_flights(api, zone_data: dict, zone_name: str = "") -> dict:
    flights = {}

    if "subzones" in zone_data:
        for subzone_name, subzone_data in zone_data["subzones"].items():
            flights.update(get_flights(api, subzone_data, f"{zone_name} - {subzone_name}"))
    else:
        bounds = api.get_bounds(zone_data)
        flights_data = api.get_flights(bounds=bounds)
        if len(flights_data) == API_LIMIT:
            subzones = divide_zone(zone_data)
            for subzone_name, subzone_data in subzones.items():
                flights.update(get_flights(api, subzone_data, f"{zone_name} - {subzone_name}"))
        else:
            flights[zone_name] = flights_data

    return flights


@task(name="Extracting flights")
def get_all_flights(api) -> list:
    print("Getting all flights")
    zones = api.get_zones()
    flights = {}
    for zone_name, zone_data in zones.items():
        flights.update(get_flights(api, zone_data, zone_name))

    all_flights = [flight for flights_in_zone in flights.values() for flight in flights_in_zone]

    return all_flights


@task(name="Creating flights raw df")
def create_flights_raw_df(flights: list, spark: SparkSession) -> DataFrame:
    """
    Create a DataFrame from the list of flights.

    Args:
        flights: list of flights
        spark: SparkSession

    Returns:
        A spark DataFrame
    """

    flights_raw_df = spark.createDataFrame(flights)
    print(f"{flights_raw_df.count()} flights found")
    return flights_raw_df


@task(name="Extracting airlines")
def get_all_airlines(api) -> list:
    return api.get_airlines()


@task(name="Extracting airports")
def get_all_airports(api) -> list:
    return api.get_airports()
