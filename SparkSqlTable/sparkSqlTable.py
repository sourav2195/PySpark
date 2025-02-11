from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSchema") \
        .enableHiveSupport()\
        .getOrCreate()

    logger = Log4J(spark)

    flightTimParquetDf = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimParquetDf.write\
        .format("csv")\
        .mode("overwrite")\
        .bucketBy(5, "ORIGIN", "OP_CARRIER")\
        .sortBy("ORIGIN", "OP_CARRIER")\
        .saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))