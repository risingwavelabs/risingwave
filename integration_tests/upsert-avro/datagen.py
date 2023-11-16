from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import sys
import random
import time


key_schema = r'''
"string"
'''
value_schema = r'''
{"type":"record","name":"OBJ_ATTRIBUTE_VALUE","namespace":"CPLM","fields":[{"name":"op_type","type":["null","string"],"default":null},{"name":"ID","type":["null","string"],"default":null},{"name":"CLASS_ID","type":["null","string"],"default":null},{"name":"ITEM_ID","type":["null","string"], "default":null},{"name":"ATTR_ID","type":["null","string"],"default":null},{"name":"ATTR_VALUE","type":["null","string"],"default":null},{"name":"ORG_ID","type":["null","string"],"default":null},{"name":"UNIT_INFO","type":["null","string"],"default":null},{"name":"UPD_TIME","type":["null","string"],"default":null}],"connect.name":"CPLM.OBJ_ATTRIBUTE_VALUE"}
'''


def create_topic(kafka_conf, topic_name):
    client = AdminClient(kafka_conf)
    topic_list = []
    topic_list.append(
        NewTopic(topic_name, num_partitions=1, replication_factor=1))
    client.create_topics(topic_list)


def get_basic_value(id):
    return [str(id), {
        "op_type": "update",
        "ID": str(id),
        "CLASS_ID": "6566",
        "ITEM_ID": "6768",
        "ATTR_ID": "6970",
        "ATTR_VALUE": "value9",
        "ORG_ID": "7172",
        "UNIT_INFO": "info9",
        "UPD_TIME": "2021-05-18T07:59:58.714Z"
    }]


def get_key_value_and_serializer(id, schema_registry_client):
    [key, value] = get_basic_value(id)
    return key, AvroSerializer(schema_registry_client=schema_registry_client, schema_str=key_schema), value, AvroSerializer(schema_registry_client=schema_registry_client, schema_str=value_schema)


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
            id += 1
            key, key_serializer, value, value_serializer = get_key_value_and_serializer(
                id, schema_registry_client)
            producer = Producer(kafka_conf)
            producer.produce(topic=topic, partition=0,
                             key=key_serializer(key,SerializationContext(topic, MessageField.KEY)),
                             value=value_serializer(
                                 value, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)
            producer.flush()
            if id % 100 == 0:
                print("Sent {} records".format(id))
            time.sleep(0.05)
