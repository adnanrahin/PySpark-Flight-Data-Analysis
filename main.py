from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local[*]")
             .appName("FlightDataAnalysis")
             .getOrCreate())

    data_source_path = '2015_flights_data'

    flights_csv = spark.sparkContext.textFile(data_source_path + '/flights.csv')
    airlines_csv = spark.sparkContext.textFile(data_source_path + '/airlines.csv')
    airport_csv = spark.sparkContext.textFile(data_source_path + '/airports.csv')

