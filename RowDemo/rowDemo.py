from pyspark.sql import SparkSession, Row
from lib.logger import Log4J
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import to_date


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkSchema") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4J(spark)

    my_schema = StructType([
        StructField("id", StringType()),
        StructField("EventDate", StringType())
    ])

    my_row = [Row("123", "03/02/2014"), Row("124", "01/01/2024"), Row("125", "01/02/2024"), Row("126", "02/02/2024")]
    my_rdd = spark.sparkContext.parallelize(my_row, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    my_df.printSchema()
    my_df.show()
    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    my_df.printSchema()
    my_df.show()
