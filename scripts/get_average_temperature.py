import argparse
import logging
import re
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        default="GlobalLandTemperaturesByMajorCity.csv",
        help=(
            ".csv file containing information"
            "about cities and temperatures per years."
        ),
    )
    parser.add_argument(
        "-c",
        "--city",
        required=True,
        help="city to calculate temperature for."
    )
    parser.add_argument(
        "-y",
        "--year",
        required=True,
        help="year to calculate temperature for."
    )
    args = parser.parse_args()
    return args


def _download_data(filename: str, session: SparkSession) -> DataFrame:
    file = Path(filename)
    if not file.is_file():
        raise ValueError(f"The file {file} does not exist!")

    if re.search(r"\.csv$", filename) is None:
        raise ValueError(f"The file {file} is not a .csv!")

    data: pd.DataFrame = pd.read_csv(filename)
    data_spark: DataFrame = session.createDataFrame(data)
    data_spark = data_spark.na.drop()
    data_spark = data_spark.select(
        f.col("City"), f.year("dt").alias("Year"), f.col("AverageTemperature")
    )
    return data_spark


def _validate_request(df: DataFrame, city: str, year: str):
    if df.filter(df.City.isin([city])).count() == 0:
        raise ValueError(f"Information about {city} "
                         "does not exists in given data!")
    if df.filter(df.Year.isin([year])).count() == 0:
        raise ValueError(
            f"Information about {city} for {year} year "
            "does not exists in given data!"
        )


def get_city_average_year_temp(df: DataFrame, city: str, year: str) -> float:
    """Get average temperature for year in city.

    Args:
        df (DataFrame): Data containing info about temperature in cities.
        city (str): Name of city.
        year (str): Year to calculate average temperature for.

    Returns:
        float: Average temperature.
    """
    _validate_request(df=df, city=city, year=year)
    logging.info("Start searching...")
    df = df.filter((df.Year == year) & (df.City == city))
    df = df.groupBy(f.col("Year")).agg(
        f.mean("AverageTemperature").alias("MeanTempPerYear")
    )
    assert df.count() == 1
    return df.first().MeanTempPerYear


def main():
    """Entry point of script calculating
    average temperature in given year for given city."""
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()
    spark: SparkSession = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    logging.info("Start downloading data...")
    try:
        data: DataFrame = _download_data(
            session=spark,
            filename=args.input_file
            )
    except ValueError as error:
        logging.fatal(error)
        return
    logging.info("Data downloaded. Ready for processing...")

    try:
        temperature: float = get_city_average_year_temp(
            df=data, city=args.city, year=args.year
        )
    except ValueError as error:
        logging.fatal(error)
        return
    logging.info(
        f"Done! Average temperature in {args.city}"
        f" in {args.year} is {temperature:.2f}."
    )


if __name__ == "__main__":
    main()
