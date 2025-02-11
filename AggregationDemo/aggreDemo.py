from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from lib.logger import Log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AggregationDemo") \
        .master("local[3]") \
        .getOrCreate()
    logger = Log4J(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    # invoice_df.printSchema()

    # simple aggregation with column object expression. Select method will return a new DF.One line summery
    invoice_df.select(f.count("*").alias("CountAll"), f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"), f.countDistinct("InvoiceNo").alias("DistinctCount")).show()

    # simple aggregation with SQL like string expression. count(1) and count(*) are same.
    invoice_df.selectExpr("count(1) as CountAll", "count(StockCode) as CountField", "sum(Quantity) as TotalQuantity",
                          "avg(UnitPrice) as AvgPrice").show()

    # Group By using sql expression
    invoice_df.createOrReplaceTempView("sales")
    summery_sql = spark.sql("""SELECT Country, InvoiceNo, sum(Quantity) as TotalQuantity, round(sum(Quantity * 
    UnitPrice),2) as InvoiceValue from sales Group by Country, InvoiceNo """)
    # summery_sql.show()

    # Group By using DF expression
    summery_df = (invoice_df.groupby("Country", "InvoiceNo")
                  .agg(f.sum("Quantity").alias("TotalQuantity"),
                       f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")))
    # summery_df.show()
    # Creating aggregated colum separately
    NumInvoices = f.countDistinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")

    # convert InvoiceDate to Date type --> filter only 2010 year --> create a new col WeekNumber
    exSummery_df = invoice_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear("InvoiceDate")) \
        .groupby("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue).orderBy("Country")
    exSummery_df.show()

    # save the file in Output folder as parquet format.
    exSummery_df.write.format("parquet").mode("overwrite").save("Output")

    """
    invoices_date_df = invoice_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy HH.mm"))
    summery_df2 = (invoices_date_df.groupBy("Country", f.weekofyear("InvoiceDate").alias("WeekNumber"))
                   .agg(f.countDistinct("InvoiceNo").alias("NumInvoices"),
                        f.sum("Quantity").alias("TotalQuantity"),
                        f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"))).orderBy("Country")
    summery_df2.show()
   """
