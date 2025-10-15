import json
import time
import sys
import paho.mqtt.client as mqtt

RABBITMQ_HOST = "rabbitmq-server"
RABBITMQ_PORT = 1883

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print(f"Connected to RabbitMQ MQTT broker successfully")
    else:
        print(f"Failed to connect, return code {reason_code}")

def on_publish(client, userdata, mid, reason_code, properties):
    print(f"Message {mid} published with reason code {reason_code}")

def produce_messages(topic: str, count: int = 100):
    """Produce JSON messages to RabbitMQ via MQTT"""
    client = mqtt.Client(client_id="rabbitmq_producer", protocol=mqtt.MQTTv5)
    client.on_connect = on_connect
    client.on_publish = on_publish
    
    try:
        client.connect(RABBITMQ_HOST, RABBITMQ_PORT, 60)
        client.loop_start()
        
        # Wait for connection
        time.sleep(1)
        
        for i in range(count):
            payload = json.dumps({"id": i, "value": f"message_{i}"})
            result = client.publish(topic, payload, qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Failed to publish message {i}: {result.rc}")
            else:
                print(f"Published message {i}: {payload}")
        
        # Wait for all messages to be published
        time.sleep(2)
        
        client.loop_stop()
        client.disconnect()
        print(f"Successfully published {count} messages to topic '{topic}'")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def produce_messages_multiple_topics(topics: list, count: int = 100):
    """Produce JSON messages to multiple topics"""
    for topic in topics:
        print(f"Producing messages to topic: {topic}")
        produce_messages(topic, count)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python rabbitmq_operation.py <command> <topic> [count]")
        print("Commands:")
        print("  produce <topic> [count]       - Produce messages to a topic (default: 100)")
        print("  produce_multi <topic1,topic2> [count] - Produce messages to multiple topics")
        sys.exit(1)

    command = sys.argv[1]

    if command == "produce":
        if len(sys.argv) < 3:
            print("Error: Topic is required")
            sys.exit(1)
        topic = sys.argv[2]
        count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        produce_messages(topic, count)
    
    elif command == "produce_multi":
        if len(sys.argv) < 3:
            print("Error: Topics are required (comma-separated)")
            sys.exit(1)
        topics = sys.argv[2].split(",")
        count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
        produce_messages_multiple_topics(topics, count)
    
    else:
        print(f"Unknown command: {command}")
        print("Supported commands: produce, produce_multi")
        sys.exit(1)

