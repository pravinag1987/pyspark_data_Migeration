import configparser
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('proj_config')

spark = SparkSession.builder.master("local").appName("Connect_MS_SQL_Server")\
         .config(config.get('MS_SQL_Server_Connection_details', 'MS_class'),config.get('MS_SQL_Server_Connection_details', 'MS_jar_path'))\
         .enableHiveSupport().getOrCreate()

database = config.get('MS_SQL_Server_Connection_details', 'MS_database')
table = config.get('MS_SQL_Server_Connection_details', 'MS_table')
user = config.get('MS_SQL_Server_Connection_details', 'MS_user')
password  = config.get('MS_SQL_Server_Connection_details', 'MS_password')
driver = config.get('MS_SQL_Server_Connection_details', 'MS_driver')

jdbcDF = spark.read.format("jdbc")\
         .option("url", "jdbc:sqlserver://localhost:1433;databaseName="+ database +"")\
         .option("dbtable", table)\
         .option("user", user)\
         .option("password", password)\
         .option("driver", driver).load()


map_col = jdbcDF.rdd.map(lambda x: x.usecase_name)

for usecase in map_col.collect():
    if usecase == "cal_manteca":
        input_dir = jdbcDF.select(jdbcDF["input_dir"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print input_dir

        output_dir = jdbcDF.select(jdbcDF["output_dir"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print output_dir

        datafile = jdbcDF.select(jdbcDF["datafile"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print datafile

        metafile = jdbcDF.select(jdbcDF["metafile"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print metafile

    elif usecase == "cal_berkeley":
        input_dir = jdbcDF.select(jdbcDF["input_dir"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print input_dir

        output_dir = jdbcDF.select(jdbcDF["output_dir"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print output_dir

        datafile = jdbcDF.select(jdbcDF["datafile"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print datafile

        metafile = jdbcDF.select(jdbcDF["metafile"]).where(jdbcDF["usecase_name"] == usecase).first()[0]
        print metafile

