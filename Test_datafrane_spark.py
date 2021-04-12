from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark =  SparkSession.builder.master("local").appName("Test_File_loading").enableHiveSupport().getOrCreate()
schema = StructType([])
df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

randomList = ['a', 0, 2]

for entry in randomList:
    try:

        r = 1/int(entry)
        break
    except Exception as e:

        errorr = str(e)

        df1 = df.withColumn("error_info", lit(errorr))
        df1.write.format("orc").mode("append").insertInto("test_spark.test_log")









