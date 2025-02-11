from datetime import date
from unittest import TestCase
from pyspark.sql import SparkSession,Row
from pyspark.sql.types import StructType, StructField, StringType
from rowDemo import to_date_df


class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder\
                    .master("local[3]")\
                    .appName("RowDemoTest")\
                    .getOrCreate()

        my_schema = StructType([
            StructField("id", StringType()),
            StructField("EventDate", StringType())
        ])

        my_row = [Row("123", "04/05/2024"), Row("124", "4/5/2024"), Row("125", "04/5/2024"),Row("126", "4/05/2024")]
        my_rdd = cls.spark.sparkContext.parallelize(my_row, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_types(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_data_values(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2024, 4, 5))

