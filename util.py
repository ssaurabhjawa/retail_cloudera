from pyspark.sql import SparkSession
def get_spark_session(env, appName):
    if env == 'DEV':
        spark = SparkSession. \
                builder. \
                master('local'). \
                appName('retail_poc'). \
                getOrcreate()
    elif env == 'PROD':
        spark = SparkSession. \
                builder. \
                master('yarn'). \
                appName('retail_poc'). \
                getOrCreate()
        return spark
    return
