#!/usr/bin/python3
import json
import random

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def get_column_name_by_connector_and_column(connector_name: str, additional_column_name: str) -> str:
    f"""_rw_{connector_name}_{additional_column_name}"""


def get_supported_additional_columns_by_connector(connector_name: str) -> [str]:
    # keep consistent with `src/connector/src/parser/additional_columns.rs`
    supported_dict = {
        "kafka": ["key", "partition", "offset", "timestamp", "header"],
        "pulsar": ["key", "partition", "offset"],
        "kinesis": ["key", "partition", "offset"],
        "s3_v2": ["file", "offset"],
        # todo: gcs
    }
    if connector_name not in supported_dict:
        raise Exception("connector {} is not supported".format(connector_name))

    return supported_dict[connector_name]


def get_connect_config_by_connector(connector_name: str) -> dict:
    connect_config_dict = {
        "kafka": {
            "topic": "test_additional_columns",
            "properties.bootstrap.server": "localhost:29092",  # redpanda docker run here
            "connector": "kafka",
        }
    }
    if connector_name not in connect_config_dict:
        raise Exception("connector {} is not supported".format(connector_name))

    return connect_config_dict[connector_name]


def create_source_with_spec_connector(connector_name: str):
    additional_columns = get_supported_additional_columns_by_connector(connector_name)
    connect_config = get_connect_config_by_connector(connector_name)

    table_name = f"{connector_name}_test_additional_column"
    base_schema = "CREATE TABLE {} (id INT, name VARCHAR, age INT) {} WITH ({}) FORMAT PLAIN ENCODE JSON"
    additional_columns_str = "".join([" include {} ".format(column) for column in additional_columns])
    connect_config_str = ", ".join(["{} = '{}'".format(k, v) for k, v in connect_config.items()])

    run_sql = base_schema.format(table_name, additional_columns_str, connect_config_str)
    print(run_sql)

    return table_name


def drop_table(table_name: str):
    sql = f"DROP TABLE IF EXISTS {table_name}"
    print(sql)


def generate_data():
    for i in range(200):
        key = "key_{}".format(i)
        gen_one_row = {
            "id": i,
            "name": "name_{}".format(i),
            "age": i % 20,
        }
        yield key, gen_one_row


def generate_kafka(connect_config: dict):
    # generate kafka key, payload, header
    broker_addr = connect_config["properties.bootstrap.server"]
    topic = connect_config["topic"]

    kafka_conf = {'bootstrap.servers': broker_addr}

    admin_client = AdminClient(kafka_conf)
    topic_list = [NewTopic(topic, num_partitions=3)]
    admin_client.create_topics(topic_list)

    producer = Producer(kafka_conf)
    header_pool = [("key1", "value1"), ("key2", "value2"), ("key3", "value3"), ("key4", "value4")]

    for key, one_row in generate_data():
        headers = random.sample(header_pool, random.randint(0, len(header_pool)))
        producer.produce(topic=topic, value=json.dumps(one_row), key=json.dumps(key), headers=headers)
    producer.flush()


def load_mq_for_ground_truth():
    pass


def load_kafka(config: dict) -> dict:

    def on_assign(consumer, partitions):
        global eof_reached
        eof_reached = {partition: False for partition in partitions}

    eof_reached = {}

    connect_conf = {
        "bootstrap.servers": config["properties.bootstrap.server"],
        "auto.offset.reset": "earliest",
        "group.id": "consumer-ci",
    }
    topic = config["topic"]
    consumer = Consumer(connect_conf)
    consumer.subscribe([topic], on_assign=on_assign)

    print(eof_reached)

    result = {}
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            print(msg)
            if msg is None:
                print("No message")
                continue
            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    eof_reached[msg.partition()] = True
                    print("End of partition reached {}/{}".format(msg.topic(), msg.partition()))
                    if all(eof_reached.values()):
                        print("All partitions EOF, exiting")
                        break
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                result[msg.key()] = {
                    # focus on the following columns
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp(),
                    "headers": msg.headers(),
                }

    finally:
        consumer.close()

    return result


if __name__ == '__main__':
    # generate_kafka(get_connect_config_by_connector("kafka"))

    res = load_kafka(get_connect_config_by_connector("kafka"))
    print(len(res))
