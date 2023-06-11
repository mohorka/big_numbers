"""Tests for the get_city_average_year_temp function."""
import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

from scripts.get_average_temperature import get_city_average_year_temp


@pytest.mark.parametrize(
    "data, city, year, expected",
    [
        (  # One city
            pd.DataFrame(
                {
                    "Year": [2013, 2013],
                    "City": ["City A", "City A"],
                    "AverageTemperature": [1.0, 2.0],
                },
            ),
            "City A",
            2013,
            1.5,
        ),
        (  # Duplicate row
            pd.DataFrame(
                {
                    "Year": [2013, 2013],
                    "City": ["City A", "City A"],
                    "AverageTemperature": [2.0, 2.0],
                },
            ),
            "City A",
            2013,
            2.0,
        ),
        (  # Two cities
            pd.DataFrame(
                {
                    "Year": [2013, 2013, 2013, 2013],
                    "City": ["City A", "City A", "City B", "City B"],
                    "AverageTemperature": [1.0, 3.0, 13.0, 15.0],
                },
            ),
            "City B",
            2013,
            14.0,
        ),
    ],
)
def test_get_city_average_year_temp(
    data: pd.DataFrame,
    city: str,
    year: int,
    expected: float,
) -> None:
    """Test that the function returns the correct values."""
    spark: SparkSession = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark_df = spark.createDataFrame(data)
    assert get_city_average_year_temp(spark_df, city, year) == expected


@pytest.mark.parametrize(
    "data, city, year, expected_error",
    [
        (  # Year not found
            pd.DataFrame(
                {
                    "Year": [2013, 2013],
                    "City": ["City A", "City A"],
                    "AverageTemperature": [1.0, 2.0],
                },
            ),
            "City A",
            2014,
            ValueError,
        ),
        (  # City not found
            pd.DataFrame(
                {
                    "Year": [2013, 2013],
                    "City": ["City A", "City A"],
                    "AverageTemperature": [1.0, 2.0],
                },
            ),
            "City B",
            2013,
            ValueError,
        ),
    ],
)
def test_get_city_average_year_temp_error(
    data,
    city,
    year,
    expected_error
) -> None:
    """Test that the function raises the correct errors."""
    spark: SparkSession = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark_df = spark.createDataFrame(data)
    with pytest.raises(expected_error):
        get_city_average_year_temp(spark_df, city, year)
