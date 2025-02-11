from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4J
#import sys
from collections import namedtuple

surveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf()\
            .setMaster("local[3]")\
            .setMaster("HelloRdd")
    #sc = SparkContext(conf = conf)

    spark = SparkSession.builder\
        .config(conf=conf)\
        .getOrCreate()
    sc = spark.sparkContext

    logger = Log4J(spark)

    linesRDD = sc.textFile("data/sample.csv")
    partionedRDD = linesRDD.repartition(2)

    colsRDD = partionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: surveyRecord(int(cols[1]), int(cols[2]), int(cols[3]), int(cols[4])))
    filterRDD = selectRDD.map(lambda r: r.Age < 40)
    kvRDD = filterRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)
    colsList = countRDD.collect()

    for x in colsList:
        logger.info(x)

