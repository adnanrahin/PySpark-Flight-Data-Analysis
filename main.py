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


if __name__ == "__main__":
    faulthandler.enable()

    spark = (SparkSession
             .builder
             .master("local[*]")
             .config("spark.executor.memory", "6G")
             .config("spark.driver.memory", "4G")
             .appName("FlightDataAnalysis")
             .getOrCreate())

    data_source_path = '2015_flights_data'


