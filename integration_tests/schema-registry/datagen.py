from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import sys
import random
import time

# the two versions of the schema are compatible
schema_v1 = r'''
{
  "name": "student",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int",
      "default": 0
    },
    {
      "name": "name",
      "type": "string",
      "default": ""
    },
    {
      "name": "avg_score",
      "type": "double",
      "default": 0.0
    },
    {
      "name": "age",
      "type": "int",
      "default": 0
    },
    {
      "name": "schema_version",
      "type": "string",
      "default": ""
    }
  ]
}
'''

schema_v2 = r'''
{
  "name": "student",
  "type": "record",
  "fields": [
    {
      "name": "id",
      "type": "int",
      "default": 0
    },
    {
      "name": "name",
      "type": "string",
      "default": ""
    },
    {
      "name": "avg_score",
      "type": "double",
      "default": 0.0
    },
    {
      "name": "age",
      "type": "int",
      "default": 0
    },
    {
      "name": "facebook_id",
      "type": "string",
      "default": ""
    },
    {
      "name": "schema_version",
      "type": "string",
      "default": ""
    }
  ]
}
'''

schemas = {'v1': schema_v1, 'v2': schema_v2}


def create_topic(kafka_conf, topic_name):
    client = AdminClient(kafka_conf)
    topic_list = []
    topic_list.append(
        NewTopic(topic_name, num_partitions=1, replication_factor=1))
    client.create_topics(topic_list)


def get_basic_value(id):
    return {'id': id, 'name': ''.join(random.sample('zyxwvutsrqponmlkjihgfedcba', 7)), 'avg_score': random.random() * 100, 'age': random.randint(10, 100)}


def get_value_and_serializer(id, version, schema_registry_client):
    value = get_basic_value(id)
    value['schema_version'] = version
    if version == 'v2':
        value['facebook_id'] = "12345678"
    return value, AvroSerializer(schema_registry_client=schema_registry_client, schema_str=schemas[version])


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.value(), err))
        return


if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("datagen.py <brokerlist> <schema-registry-url> <topic>")
    broker_list = sys.argv[1]
    schema_registry_url = sys.argv[2]
    topic = sys.argv[3]

    print("broker_list: {}".format(broker_list))
    print("schema_registry_url: {}".format(schema_registry_url))
    print("topic: {}".format(topic))

    schema_registry_conf = {'url': schema_registry_url}
    kafka_conf = {'bootstrap.servers': broker_list}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    create_topic(kafka_conf=kafka_conf, topic_name=topic)

    id = 0
    while True:
        for version in schemas.keys():
            id += 1
            value, avro_serializer = get_value_and_serializer(
                id, version, schema_registry_client)
            producer = Producer(kafka_conf)
            producer.produce(topic=topic, partition=0,
                             value=avro_serializer(
                                 value, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            producer.flush()
            if id % 100 == 0:
                print("Sent {} records".format(id))
            time.sleep(0.05)
