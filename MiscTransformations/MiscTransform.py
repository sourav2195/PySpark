from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MiscDemo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4J(spark)

    dataList = [("Ravi", "28", "1", "2002"),
                ("Abdul", "23", "5", "81"),
                ("John", "12", "12", "6"),
                ("Rosy", "7", "8", "63"),
                ("Abdul", "23", "5", "81")
                ]
    raw_df = spark.createDataFrame(dataList).toDF("Name", "Day", "Month", "Year")
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    # raw_df.printSchema()
    # df1.show()

    df2 = df1.withColumn("year", expr("""
                        case when year < 25 then year + 2000
                        when year < 100 then year + 1900
                        else year
                        end"""))
    # df2.show()

    # Inline Casting, year will be still Sting type but no decimal number will appear.
    df3 = df1.withColumn("year", expr("""
                        case when year < 25 then cast(year as int) + 2000
                        when year < 100 then cast(year as int) + 1900
                        else year
                        end"""))
    # df3.printSchema()
    # df3.show()

    # Casting the Schema, in this case Year will be changed to Integer type
    df4 = df1.withColumn("year", expr("""
                        case when year < 25 then year + 2000
                        when year < 100 then year + 1900
                        else year
                        end""").cast(IntegerType()))
    # df4.printSchema()
    # df4.show()

    # casting the col with appropriate datatype
    df5 = df1.withColumn("Day", col("Day").cast(IntegerType())) \
        .withColumn("Month", col("Month").cast(IntegerType())) \
        .withColumn("Year", col("year").cast(IntegerType()))
    df6 = df5.withColumn("year", expr("""
                        case when year < 25 then year + 2000
                        when year < 100 then year + 1900
                        else year
                        end"""))
    # df6.show()
    # df6.printSchema()

    # column object expression for CASE expression
    df7 = df5.withColumn("year", \
                         when(col("year") < 25, col("year") + 2000)
                         .when(col("year") < 100, col("year") + 1900)
                         .otherwise(col("year")))
    # df7.show()

    # Adding another column DOB
    df8 = df7.withColumn("DOB", expr("to_date(concat(Day,'/', Month, '/', Year),'d/M/y')"))
    # df8.show()
    # df8.printSchema()

    # Drop unnecessary colum like Day, Month and Year and drop duplicate rows and sort them by dob
    df9 = df7.withColumn("DOB", expr("to_date(concat(Day,'/', Month, '/', Year),'d/M/y')")) \
        .drop("day", "month", "year")\
        .dropDuplicates(["name", "dob"])\
        .sort(expr("dob desc"))
    df9.show()
