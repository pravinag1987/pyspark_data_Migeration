import os
import configparser
from pyspark.sql import SparkSession
from datetime import datetime
from subprocess import call

config = configparser.ConfigParser()
config.read('proj_config')

## Send as parameter
file_path = config.get('project_settings', 'file_pathh')
## Send as parameter
scriptt = config.get('project_settings', 'ddl_script')
spark =  SparkSession.builder.master("local").appName("Test_Load_partition_tbl").enableHiveSupport().getOrCreate()
## Send as parameter
raw_dataa = spark.sql(config.get('project_settings', 'raw_tab_query'))

## Hive table name have send as parameter
get_hive_dt = spark.sql("select max(load_date) from "+ config.get('project_settings', 'raw_table') +"").first()[0]

current_dt = datetime.now().strftime("%Y-%m-%d")

if str(current_dt) == str(get_hive_dt):
  ## Send as parameter
  raw_dataa.write.format("orc").mode("overwrite").insertInto(config.get('project_settings', 'partition_table'))
else:
  ## Send as parameter (After insert into final partition table we need to drop raw table
  spark.sql("drop table "+ config.get('project_settings', 'raw_table') +"")
  ## Send as parameter (Again creat empty raw table)
  call('hive -f "'+ file_path + scriptt +'"', shell=True)
