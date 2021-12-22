from pyspark.sql import SparkSession
from pyspark import SparkContext

if __name__ == "__main__":
    spark = (SparkSession
             .builder
             .master("local[*]")
             .config("spark.executor.memory", "32G")
             .config("spark.driver.memory", "32G")
             .appName("FlightDataAnalysis")
             .getOrCreate())

    data_source_path = '2015_flights_data'

    flights_csv = (spark
                   .sparkContext
                   .textFile(data_source_path + '/flights.csv')
                   .filter(lambda row: row is not None)
                   .filter(lambda row: row != ""))

    airlines_csv = (spark
                    .sparkContext
                    .textFile(data_source_path + '/airlines.csv')
                    .filter(lambda row: row is not None)
                    .filter(lambda row: row != ""))

    airport_csv = (spark
                   .sparkContext
                   .textFile(data_source_path + '/airports.csv')
                   .filter(lambda x: x is not None)
                   .filter(lambda x: x != ""))

    print(flights_csv.first())
