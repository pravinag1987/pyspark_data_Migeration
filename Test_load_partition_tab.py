import os
from pyspark.sql import SparkSession
from datetime import datetime
from subprocess import call

## Send as parameter
file_path = '/home/hduser1/PravinFiles/Sparkfiles/data/AirLineData/Test_Input_data/'
## Send as parameter
scriptt = 'tbl_airline_data_orc.ddl'
spark =  SparkSession.builder.master("local").appName("Test_Load_partition_tbl").enableHiveSupport().getOrCreate()
## Send as parameter
raw_dataa = spark.sql("select * from test_spark.tbl_airline_data_orc")

## Hive table name have send as parameter
get_hive_dt = spark.sql("select max(load_date) from test_spark.tbl_airline_data_orc").first()[0]

current_dt = datetime.now().strftime("%Y-%m-%d")

if str(current_dt) == str(get_hive_dt):
  ## Send as parameter
  raw_dataa.write.format("orc").mode("overwrite").insertInto("test_spark.tbl_airline_data_orc_par")
else:
  ## Send as parameter (After insert into final partition table we need to drop raw table
  spark.sql("drop table test_spark.tbl_airline_data_orc")
  ## call unix shell to run hive command (Again creat empty raw table)
  call('hive -f "'+ file_path + scriptt +'"', shell=True)
