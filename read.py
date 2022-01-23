from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from pyspark.sql import SparkSession
import json

import getpass
username = getpass.getuser()

admin_client = KafkaAdminClient(
    bootstrap_servers="cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com",
    client_id='test'
)

topic_list = []
topic_list.append(NewTopic(name="retail_topic", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)






filename='/home/saurabh/test_source/part-00000'
f = open(filename)
data = json.load(f)
f.close()

producer = KafkaProducer(security_protocol="SSL", bootstrap_servers =['cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com'],
                        value_serializer=lambda x:
                        dumps(x).encode('utf-8'))
for t in topic_list:
    producer.send(t, data.encode('utf-8'))
    producer.flush()

spark = SparkSession. \
    builder. \
    config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1'). \
    config('spark.ui.port', '0'). \
    config('spark.sql.warehouse.dir', '/user/saurabh/warehouse'). \
    enableHiveSupport(). \
    appName('Python - Kafka and Spark Integration'). \
    master('yarn'). \
    getOrCreate()

kafka_bootstrap_servers = 'cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com'

df = spark. \
  readStream. \
  format('kafka'). \
  option('kafka.bootstrap.servers', kafka_bootstrap_servers). \
  option('subscribe', topicName). \
  load()

df.isStreaming
df.printSchema()