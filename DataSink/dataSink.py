from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSchema") \
        .getOrCreate()

    logger = Log4J(spark)

    flightTimParquetDf = spark.read \
        .format("parquet") \
        .load("data/flight*.parquet")

    logger.info("No of partition before: " + str(flightTimParquetDf.rdd.getNumPartitions()))
    flightTimParquetDf.groupBy(spark_partition_id()).count().show()

    partitionedDF = flightTimParquetDf.repartition(5)
    logger.info("No of partition after: " + str(partitionedDF.rdd.getNumPartitions()))
    partitionedDF.groupBy(spark_partition_id()).count().show()
    '''
    partitionedDF.write \
        .format("avro") \
        .mode("overwrite") \
        .option("path", "dataSink/avro/") \
        .save() '''
    flightTimParquetDf.write\
        .format("json")\
        .mode("overwrite")\
        .option("path", "dataSink/json/")\
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .option("maxRecordsPerFile", 1000)\
        .save()


