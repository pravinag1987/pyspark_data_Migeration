from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark =  SparkSession.builder.master("local").appName("Test_File_loading").enableHiveSupport().getOrCreate()
schema = StructType([])

randomList = ['a', 0, 2]

for entry in randomList:
    try:

        r = 1/int(entry)
        break
    except Exception as e:

        err_data = repr(e)

        df = spark.createDataFrame([err_data], StringType())

        df.write.mode("append").insertInto("test_spark.test_log")









