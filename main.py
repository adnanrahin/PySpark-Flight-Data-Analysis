from pyspark.sql import SparkSession
from pyspark import SparkContext
import faulthandler


def load_data_set_to_rdd(path, spark):
    rdd = (spark
           .sparkContext
           .textFile(path)
           .filter(lambda row: row is not None)
           .filter(lambda row: row != ""))
    return rdd


def data_writer(df, mode, path):
    (cancelled_flight_df
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


def find_all_that_canceled(flightDF):
    canceled_flight = (flightDF
                       .select('*')
                       .where('CANCELLED = 1'))

    return canceled_flight


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

    cancelled_flight_df = find_all_that_canceled(flightDF)
    data_writer(cancelled_flight_df, 'overwrite', './transform_data/cancelled_flights')

    spark.stop()
