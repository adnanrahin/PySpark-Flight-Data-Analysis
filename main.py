import configparser
import sys
from pyspark.rdd import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkContext, SparkConf
import faulthandler
from pyspark.sql.functions import col, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import Row
from pyspark.sql import Column
from pyspark.sql.functions import sum
import constant as const


def load_data_set_to_rdd(path: str, spark: SparkSession) -> RDD:
    rdd = (spark
           .sparkContext
           .textFile(path)
           .filter(lambda row: row is not None)
           .filter(lambda row: row != ""))

    return rdd


def data_writer_parquet(df: DataFrame, mode: str, path: str) -> None:
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
        flightDF.join(airlineDF.withColumnRenamed(const.AIRLINE, const.AIRLINE_NAME),
                      flightDF[const.AIRLINE] == airlineDF[const.IATA_CODE],
                      'inner')
    )

    all_cancelled_flights = (
        join_airline_flights_df
            .select('*')
            .where('CANCELLED = 1')
    )

    airline_and_number_flights_cancelled = (
        all_cancelled_flights
            .groupby(const.AIRLINE_NAME)
            .count()
            .withColumnRenamed('count', 'TOTAL_NUMBER_FLIGHTS_CANCELLED')
            .orderBy('TOTAL_NUMBER_FLIGHTS_CANCELLED')
    ).collect()

    schema = StructType(
        [
            StructField(const.AIRLINE_NAME, StringType(), True),
            StructField(const.TOTAL_NUMBER_OF_FLIGHTS_CANCELLED, StringType(), True)
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
                                   all_departure_flights[const.ORIGIN_AIRPORT] == airportDF[const.IATA_CODE],
                                   'inner')
    )

    total_departure_flights_from_each_airport = (
        join_flights_and_airports
            .groupby('AIRPORT')
            .count()
            .withColumnRenamed('count', const.TOTAL_NUMBER_DEPARTURE_FLIGHTS)
            .orderBy(const.TOTAL_NUMBER_DEPARTURE_FLIGHTS)
    ).collect()

    schema = StructType(
        [
            StructField(const.AIRPORT_NAME, StringType(), True),
            StructField(const.TOTAL_NUMBER_DEPARTURE_FLIGHTS, StringType(), True)
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
        cancelled_flights.join(airlineDF.withColumnRenamed(const.AIRLINE, const.AIRLINE_NAME)
                               , flightDF[const.AIRLINE] == airlineDF[const.IATA_CODE], 'inner')
    )

    find_max_cancelled_airline = (
        join_flights_and_airline.groupby(const.AIRLINE_NAME)
            .count()
            .withColumnRenamed('count', const.TOTAL_NUMBER_CANCELLED_FLIGHTS)
            .orderBy(col(const.TOTAL_NUMBER_CANCELLED_FLIGHTS).desc())
            .limit(1)
    ).collect()

    schema = StructType(
        [
            StructField(const.AIRLINE_NAME, StringType(), True),
            StructField(const.TOTAL_NUMBER_CANCELLED_FLIGHTS, StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=find_max_cancelled_airline, schema=schema)

    return df


def find_total_distance_flown_each_airline(flightDF: DataFrame, airlineDF: DataFrame) -> DataFrame:
    cancelled_flights = flightDF.select('*').where('CANCELLED == 1')

    join_flights_and_airline = (
        cancelled_flights.join(airlineDF.withColumnRenamed(const.AIRLINE, const.AIRLINE_NAME)
                               , flightDF[const.AIRLINE] == airlineDF[const.IATA_CODE], 'inner')
    )

    columns_name = join_flights_and_airline.columns

    filter_col = list(filter(lambda x: x != const.AIRLINE_NAME and x != const.DISTANCE, columns_name))

    new_df = join_flights_and_airline.drop(*filter_col)

    total_distance_flown = (
        new_df
            .groupby(col(const.AIRLINE_NAME))
            .agg(sum(const.DISTANCE).alias(const.TOTAL_DISTANCE))
            .orderBy(const.TOTAL_DISTANCE, ascending=False)
    ).collect()

    schema = StructType(
        [
            StructField(const.AIRLINE_NAME, StringType(), True),
            StructField(const.TOTAL_DISTANCE, StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=total_distance_flown, schema=schema)

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
        data_writer_parquet(cancelled_flight_df, 'overwrite',
                            './transform_data/cancelled_flights')

    elif sys.argv[1] == '2':
        total_flight_cancelled_by_airline_name = find_airlines_total_number_of_flights_cancelled(flightDF=flightDF,
                                                                                                 airlineDF=airlineDF)
        data_writer_parquet(total_flight_cancelled_by_airline_name, 'overwrite',
                            './transform_data/airline_total_flights_cancelled')

    elif sys.argv[1] == '3':
        total_departure_flights_from_each_airport = find_total_number_of_departure_flight_from_airport_to(
            flightDF=flightDF, airportDF=airportDF)
        data_writer_parquet(total_departure_flights_from_each_airport, 'overwrite',
                            './transform_data/total_number_departure_flights')

    elif sys.argv[1] == '4':
        total_departure_flights_from_each_airport = find_max_flight_cancelled_airline(
            flightDF=flightDF, airlineDF=airlineDF)
        data_writer_parquet(total_departure_flights_from_each_airport, 'overwrite',
                            './transform_data/most_cancelled_flights_airline')

    elif sys.argv[1] == '5':
        total_distance_flown = find_total_distance_flown_each_airline(
            flightDF=flightDF, airlineDF=airlineDF)
        data_writer_parquet(total_distance_flown, 'overwrite',
                            './transform_data/total_distance_flown_each_airline')

    elif sys.argv[1] == '6':
        filter_flight_data = (
            flightDF.filter(flightDF.DEPARTURE_DELAY is not None and flightDF.DEPARTURE_DELAY > 0)
        )

        join_flights_and_airline = (
            filter_flight_data.join(airlineDF.withColumnRenamed(const.AIRLINE, const.AIRLINE_NAME)
                                    , flightDF[const.AIRLINE] == airlineDF[const.IATA_CODE], 'inner')
        )

        columns_name = join_flights_and_airline.columns

        filter_col = list(filter(lambda x: x != const.AIRLINE_NAME and x != const.DEPARTURE_DELAY, columns_name))

        delayed_flights_df = join_flights_and_airline.drop(*filter_col)

        df = (
            delayed_flights_df
                .groupby(const.AIRLINE_NAME)
                .agg(sum(const.DEPARTURE_DELAY), count(const.AIRLINE_NAME))
        )

        df.show(10, truncate=True)

    spark.stop()
