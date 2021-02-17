import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from datetime import datetime

uschema = StructType([\
    StructField("DataExtractDate",StringType(),True), \
    StructField("ReportPeriod",StringType(),True), \
    StructField("Arrival_Departure",StringType(),True), \
    StructField("Domestic_International",StringType(),True), \
    StructField("FlightType",StringType(),True), \
    StructField("Passenger_Count",IntegerType(),True) \
])

spark =  SparkSession.builder.master("local").appName("Test_File_loading").enableHiveSupport().getOrCreate()
## Send as parameter
file_path = '/home/hduser1/PravinFiles/Sparkfiles/data/AirLineData/Test_Input_data/'

for file in os.listdir(file_path):
   file_name, file_extension = os.path.splitext(file)

   filess = "file://" + file_path + file_name + file_extension

   if file_extension == ".csv":
      file_read = spark.read.option("header", "true").schema(uschema).csv(filess)
      file_read1 = file_read.withColumn("load_date", current_date())
      ## Hive table name have send as parameter
      get_hive_dt = spark.sql("select max(load_date) from test_spark.tbl_airline_data_orc").first()[0]

      current_dt = datetime.now().strftime("%Y-%m-%d")

      if (str(get_hive_dt) == 'None') or (str(current_dt) == str(get_hive_dt)):

         file_read1.write.format("orc").mode("append").insertInto("test_spark.tbl_airline_data_orc")
