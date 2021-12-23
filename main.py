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


def loading_data_set_to_df(path, spark):
    df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(path))

    return df


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

    flightDF.printSchema()

    spark.stop()
