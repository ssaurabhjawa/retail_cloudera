from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from pyspark.sql import SparkSession
import json

import getpass
username = getpass.getuser()

filename='/home/saurabh/test_source/part-00000'

with open(filename, 'w') as json_file:
    json.dump(list, json_file)

with open(json_file, "r") as read_json_file:
    data = json.load(read_json_file)
print(data)


admin_client = KafkaAdminClient(
    bootstrap_servers="cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com:2181",
    client_id='test'
)

producer = KafkaProducer(security_protocol="SSL", bootstrap_servers =['cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com:2181'],
                        value_serializer=json.dumps(data).encode('utf-8'))

topic_list = []
topic_list.append(NewTopic(name="retail_topic", num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)


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

    kafka_bootstrap_servers = 'cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com:2181'

    df = spark. \
      readStream. \
      format('kafka'). \
      option('kafka.bootstrap.servers', kafka_bootstrap_servers). \
      option('subscribe', t). \
      load()

    df.isStreaming
    df.printSchema()