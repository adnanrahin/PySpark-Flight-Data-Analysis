import configparser
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import faulthandler
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def load_data_set_to_rdd(path: str, spark: SparkSession):
    rdd = (spark
           .sparkContext
           .textFile(path)
           .filter(lambda row: row is not None)
           .filter(lambda row: row != ""))

    return rdd


def data_writer(df: DataFrame, mode: str, path: str):
    (df
     .write
     .mode(mode)
     .parquet(path))


def loading_data_set_to_df(path: str, spark: SparkSession) -> DataFrame:
    df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(path))

    return df


def find_all_the_flight_that_canceled(flightDF: DataFrame) -> DataFrame:
    canceled_flight = (flightDF
                       .select('*')
                       .where('CANCELLED = 1'))

    return canceled_flight


def find_airlines_total_number_of_flights_cancelled(flightDF: DataFrame, airlineDF: DataFrame) -> DataFrame:
    join_airline_flights_df = (
        flightDF.join(airlineDF.withColumnRenamed('AIRLINE', 'AIRLINE_NAME'),
                      flightDF['AIRLINE'] == airlineDF['IATA_CODE'],
                      'inner')
    )

    all_cancelled_flights = (
        join_airline_flights_df
            .select('*')
            .where('CANCELLED = 1')
    )

    airline_and_number_flights_cancelled = (
        all_cancelled_flights
            .groupby('AIRLINE_NAME')
            .count()
            .withColumnRenamed('count', 'TOTAL_NUMBER_FLIGHTS_CANCELLED')
            .orderBy('TOTAL_NUMBER_FLIGHTS_CANCELLED')
    ).collect()

    schema = StructType(
        [
            StructField("AIRLINE_NAME", StringType(), True),
            StructField("TOTAL_NUMBER_OF_FLIGHTS_CANCELLED", StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=airline_and_number_flights_cancelled, schema=schema)

    return df


def find_total_number_of_departure_flight_from_airport_to(flightDF: DataFrame, airportDF: DataFrame) -> DataFrame:
    all_departure_flights = (
        flightDF
            .select('*')
            .where('CANCELLED != 1')
    )

    join_flights_and_airports = (
        all_departure_flights.join(airportDF,
                                   all_departure_flights['ORIGIN_AIRPORT'] == airportDF['IATA_CODE'],
                                   'inner')
    )

    total_departure_flights_from_each_airport = (
        join_flights_and_airports
            .groupby('AIRPORT')
            .count()
            .withColumnRenamed('count', 'TOTAL_NUMBER_DEPARTURE_FLIGHTS')
            .orderBy('TOTAL_NUMBER_DEPARTURE_FLIGHTS')
    ).collect()

    schema = StructType(
        [
            StructField("AIRPORT_NAME", StringType(), True),
            StructField("TOTAL_NUMBER_DEPARTURE_FLIGHTS", StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=total_departure_flights_from_each_airport, schema=schema)

    return df


def find_max_flight_cancelled_airline(flightDF: DataFrame, airlineDF: DataFrame) -> DataFrame:
    cancelled_flights = (
        flightDF
            .select('*')
            .where('CANCELLED = 1')
    )

    join_flights_and_airline = (
        cancelled_flights.join(airlineDF.withColumnRenamed('AIRLINE', 'AIRLINE_NAME')
                               , flightDF['AIRLINE'] == airlineDF['IATA_CODE'], 'inner')
    )

    find_max_cancelled_airline = (
        join_flights_and_airline.groupby('AIRLINE_NAME')
            .count()
            .withColumnRenamed('count', 'TOTAL_NUMBER_CANCELLED_FLIGHTS')
            .orderBy(col('TOTAL_NUMBER_CANCELLED_FLIGHTS').desc())
            .limit(1)
    ).collect()

    schema = StructType(
        [
            StructField("AIRLINE_NAME", StringType(), True),
            StructField("TOTAL_NUMBER_CANCELLED_FLIGHTS", StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=find_max_cancelled_airline, schema=schema)

    return df


if __name__ == "__main__":

    if len(sys.argv) != 2:
        raise ValueError('Please Enter A Valid Number to Generate Transformed Data.')
    faulthandler.enable()

    config = configparser.ConfigParser()

    conf = SparkConf().setAppName('FlightDataAnalysis').setMaster('local[*]')

    conf.set('spark.executor.memory', '16G').set('spark.driver.memory', '16G')

    sc = SparkContext(conf=conf)

    spark = (SparkSession
             .builder.config(conf=conf)
             # .master("local[*]")
             # .config("spark.executor.memory", "16G")
             # .config("spark.driver.memory", "16G")
             # .appName("FlightDataAnalysis")
             .getOrCreate())

    data_source_path = '2015_flights_data'

    flightDF = loading_data_set_to_df(path=data_source_path + '/flights.csv', spark=spark)
    airlineDF = loading_data_set_to_df(path=data_source_path + '/airlines.csv', spark=spark)
    airportDF = loading_data_set_to_df(path=data_source_path + '/airports.csv', spark=spark)

    if sys.argv[1] == '1':
        cancelled_flight_df = find_all_the_flight_that_canceled(flightDF=flightDF)
        data_writer(cancelled_flight_df, 'overwrite', './transform_data/cancelled_flights')

    elif sys.argv[1] == '2':
        total_flight_cancelled_by_airline_name = find_airlines_total_number_of_flights_cancelled(flightDF=flightDF,
                                                                                                 airlineDF=airlineDF)
        data_writer(total_flight_cancelled_by_airline_name, 'overwrite',
                    './transform_data/airline_total_flights_cancelled')

    elif sys.argv[1] == '3':
        total_departure_flights_from_each_airport = find_total_number_of_departure_flight_from_airport_to(
            flightDF=flightDF, airportDF=airportDF)
        data_writer(total_departure_flights_from_each_airport, 'overwrite',
                    './transform_data/total_number_departure_flights')

    elif sys.argv[1] == '4':
        total_departure_flights_from_each_airport = find_max_flight_cancelled_airline(
            flightDF=flightDF, airlineDF=airlineDF)
        data_writer(total_departure_flights_from_each_airport, 'overwrite',
                    './transform_data/most_cancelled_flights_airline')

    spark.stop()
