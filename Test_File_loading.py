import os
import configparser
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from datetime import datetime

config = configparser.ConfigParser()
config.read('proj_config')

## Need to enable this block of code

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
##file_path = '/home/hduser1/PravinFiles/Sparkfiles/data/AirLineData/Test_Input_data/'
file_path = config.get('project_settings', 'file_pathh')

for file in os.listdir(file_path):
   file_name, file_extension = os.path.splitext(file)

   filess = "file://" + file_path + file_name + file_extension

   if file_extension == ".csv":
      file_read = spark.read.option("header", "true").schema(uschema).csv(filess)
      file_read1 = file_read.withColumn("load_date", current_date())
      ## Hive table name have send as parameter
      ##get_hive_dt = spark.sql("select max(load_date) from test_spark.tbl_airline_data_orc").first()[0]
      get_hive_dt = spark.sql("select max(load_date) from "+ config.get('project_settings', 'raw_table') +"" ).first()[0]

      current_dt = datetime.now().strftime("%Y-%m-%d")

      if (str(get_hive_dt) == 'None') or (str(current_dt) == str(get_hive_dt)):

         file_read1.write.format("orc").mode("append").insertInto(config.get('project_settings', 'raw_table'))
















##from pyspark import SparkConf, SparkContext
##sc = SparkContext(master="local",appName="Spark Demo")
##print(sc.textFile('file:///home/hduser1/PravinFiles/Sparkfiles/data/AirLineData/Test_Input_data/Los_Angeles_International_Airport_-_Passenger_Count_By_Carrier_Type.csv').first())