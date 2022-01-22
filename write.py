
from pyspark.sql import SparkSession

import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port', '0'). \
    config("spark.sql.warehouse.dir", "/user/saurabh/warehouse"). \
    enableHiveSupport(). \
    appName('Python - Data Processing - Overview'). \
    master('yarn'). \
    getOrCreate()


spark. \
    read. \
    json('/home/saurabh/test_source/'). \
    show()