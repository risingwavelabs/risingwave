from confluent_kafka import Producer
import socket

conf = {'bootstrap.servers': "127.0.0.1:29092",
        'client.id': socket.gethostname()}

producer = Producer(conf)

producer.produce("kafka_1_partition_topic", key="", value="{\"v1\": 11231435000004324}")
producer.produce("kafka_1_partition_topic", key="", value="{\"v1\": 5, \"v2\": \"hello\"}")

producer.poll(1)
