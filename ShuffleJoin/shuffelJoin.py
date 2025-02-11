from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder\
            .appName("Shuffle Join")\
            .master("local[3]")\
            .getOrCreate()

    logger = Log4J(spark)

    flight_time_df1 = spark.read.json("data/d1")
    flight_time_df2 = spark.read.json("data/d2")

    # To get 3 partition after shuffle, which means 3 reduce exchanges
    spark.conf.set("spark.sql.shuffle.partitions",3)

    # Shuffle join
#    join_expr = flight_time_df1.id == flight_time_df2.id
#    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    # Broadcast Join
    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner")

    # create a dummy action to understand the shuffle join
    join_df.foreach(lambda f: None)
    input("Press any key to stop.....")


