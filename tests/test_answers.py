import pytest
from pyspark.sql import SparkSession, Row
from src.answers import get_airline_with_most_flights, get_most_active_airline_per_continent, flight_with_longest_trajectory, get_average_flight_length_per_continent, get_top_three_aircraft_model_per_country

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName('pytest').getOrCreate()

@pytest.fixture(scope="session")
def test_data_df(spark):
    return spark.read.parquet('tests/test.parquet')

def test_get_airline_with_most_flights(test_data_df):
    result = get_airline_with_most_flights(test_data_df)
    assert result.airline_name == 'United Airlines'
    assert result['count'] == 81

def test_get_most_active_airline_per_continent(test_data_df):
    result = get_most_active_airline_per_continent(test_data_df)
    expected_result = {
        'Africa': 'Ethiopian Airlines',
        'Asia': 'Turkish Airlines',
        'Europe': 'Norwegian',
        'North America': 'American Airlines',
        'Oceania': 'Nauru Airlines',
        'South America': 'Avianca'
    }
    assert result == expected_result
    pass

def test_flight_with_longest_trajectory(test_data_df):
    result = flight_with_longest_trajectory(test_data_df)
    assert result.id == '34d26b57'
    assert result.aircraft_code == 'A359'
    assert result.origin_airport_iata == 'SIN'
    assert result.destination_airport_iata == 'JFK'
    assert result.number == 'SQ24'
    assert result.distance == 15340.56640625

def test_get_average_flight_length_per_continent(test_data_df):
    result = get_average_flight_length_per_continent(test_data_df)
    expected_result = {
        'Europe': 3754.54,
        'Africa': 4011.91,
        'North America': 3745.53,
        'South America': 4386.82,
        'Oceania': 4332.03,
        'Asia': 4750.06
    }
    assert result == expected_result

def test_get_top_three_aircraft_model_per_country(test_data_df):
    result = get_top_three_aircraft_model_per_country(test_data_df)
    excepted_result = {
        'Morocco': 'B78X',
        'United States': 'A321, B77W, B77L',
        'Singapore': 'A359, B77W, A388',
        'Norway': 'B738, B77L, A333',
        'Brazil': 'B77L, B748, A359',
        'Australia': 'B789, A333, A359',
        'Turkey': 'B789, A359, B77W'
    }
    for country, expected_aircrafts in excepted_result.items():
        assert result[country] == expected_aircrafts
    