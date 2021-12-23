from pyspark.sql import SparkSession
from pyspark import SparkContext
import faulthandler
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def load_data_set_to_rdd(path, spark):
    rdd = (spark
           .sparkContext
           .textFile(path)
           .filter(lambda row: row is not None)
           .filter(lambda row: row != ""))

    return rdd


def data_writer(df, mode, path):
    (df
     .write
     .mode(mode)
     .parquet(path))


def loading_data_set_to_df(path, spark):
    df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(path))

    return df


def find_all_the_flight_that_canceled(flightDF):
    canceled_flight = (flightDF
                       .select('*')
                       .where('CANCELLED = 1'))

    return canceled_flight


def find_airlines_total_number_of_flights_cancelled(flightDF, airlineDF):
    join_airline_flights_df = (
        flightDF.join(airlineDF.withColumnRenamed('AIRLINE', 'AIRLINE_NAME'),
                      flightDF.AIRLINE == airlineDF.IATA_CODE,
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
            .withColumn('TOTAL_NUMBER_FLIGHTS_CANCELLED', col('TOTAL_NUMBER_FLIGHTS_CANCELLED')
                        .cast(IntegerType()))
    )

    return airline_and_number_flights_cancelled


if __name__ == "__main__":
    faulthandler.enable()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .config("spark.executor.memory", "16G")
             .config("spark.driver.memory", "16G")
             .appName("FlightDataAnalysis")
             .getOrCreate())

    data_source_path = '2015_flights_data'

    flightDF = loading_data_set_to_df(path=data_source_path + '/flights.csv', spark=spark)
    airlineDF = loading_data_set_to_df(path=data_source_path + '/airlines.csv', spark=spark)
    airportDF = load_data_set_to_rdd(path=data_source_path + '/airports.csv', spark=spark)

    cancelled_flight_df = find_all_the_flight_that_canceled(flightDF)
    data_writer(cancelled_flight_df, 'overwrite', './transform_data/cancelled_flights')

    total_flight_cancelled_by_airline_name = find_airlines_total_number_of_flights_cancelled(flightDF=flightDF,
                                                                                             airlineDF=airlineDF)
    data_writer(total_flight_cancelled_by_airline_name, 'overwrite', './transform_data/airline_total_flights_cancelled')

    total_flight_cancelled_by_airline_name.show(10, truncate=True)

    spark.stop()
