from kafka import KafkaConsumer
consumer = KafkaConsumer('sample')
for message in consumer:
    print (message)

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='cdp01.itversity.com:2181,cdp02.itversity.com:2181,cdp03.itversity.com:2181')
producer.send('sample', b'Hello, World!')
producer.send('sample', key=b'message-two', value=b'This is Kafka-Python')

