from pyspark.sql import *
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from lib.logger import Log4J
import re


def parse_gender(gender):
    female = r"^f$|f.m|w.m"
    male = r"^m$|ma|m.l"
    if re.search(female, gender.lower()):
        return "Female"
    elif re.search(male, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4J(spark)

    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("data/survey.csv")

    survey_df.show(10)
    # using column object expression
    # we have to register our user define function(UDF) to python function using the function name. DataFrame UDf
    parse_gender_udf = udf(parse_gender, StringType())
    logger.info("Catalog Entry: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # using sql expression but the UDF registration process is different
    # have to register as sql function and need to go to CataLog
    spark.udf.register("parse_gender_sql_udf", parse_gender, StringType())
    logger.info("Catalog Entry: ")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_sql_udf(Gender)"))
    survey_df3.show(10)

