import sys

from pyspark.sql import *
from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_survey_df, count_by_country

if __name__ == "__main__":
    #print("Starting hello Spark")
    conf = get_spark_app_config()
    spark = SparkSession.builder \
            .config(conf = conf) \
            .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) !=2:
        logger.error("Usage: Hello Spark <filename>")
        sys.exit(-1)

    logger.info("Staring HelloSpark")

    #conf_out = spark.sparkContext.getConf()
    #print(f'Config Output : {conf_out}')
    #logger.info(conf_out.toDebugString())

    survey_df = load_survey_df(spark, sys.argv[1])
    partitioned_survey_df = survey_df.repartition(2)

    count_df = count_by_country(partitioned_survey_df)

    count_df.show()
    #print(sys.argv)

    logger.info(count_df.collect())

    input("Press Enter") #only for local debugging not for Prod

    logger.info("Finishing HelloSpark")

    #spark.stop()