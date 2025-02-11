from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Window_Aggregation") \
        .master("local[2]") \
        .getOrCreate()
    logger = Log4J(spark)

    summery_df = spark.read.parquet("Data/summary.parquet")

    # creating a window
    running_window = Window.partitionBy("Country").orderBy("WeekNumber") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # create a new col RunningTotal and apply the window
    summery_df.withColumn("RunningTotal", f.round(f.sum("InvoiceValue").over(running_window), 2)).show()




