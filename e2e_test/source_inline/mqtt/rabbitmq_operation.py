import json
import sys
import time
import socket
import paho.mqtt.client as mqtt

RABBITMQ_HOST = "rabbitmq-server"
RABBITMQ_PORT = 1883


def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for when the client connects to the broker."""
    if reason_code == 0:
        print("Connected to RabbitMQ MQTT broker successfully")
    else:
        print(f"Failed to connect, return code {reason_code}", file=sys.stderr)
        sys.exit(1)


def produce_messages(topic: str, count: int):
    """Produce JSON messages to RabbitMQ via MQTT."""
    # Check if host is reachable
    try:
        socket.gethostbyname(RABBITMQ_HOST)
    except socket.gaierror as e:
        print(f"Error: Cannot resolve hostname '{RABBITMQ_HOST}': {e}", file=sys.stderr)
        sys.exit(1)

    client = mqtt.Client(
        client_id=f"rabbitmq_producer_{int(time.time())}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect

    try:
        print(f"Connecting to {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
        client.connect(RABBITMQ_HOST, RABBITMQ_PORT, 60)
        client.loop_start()

        # Wait for connection to establish
        time.sleep(1)

        print(f"Publishing {count} messages to topic '{topic}'...")
        for i in range(count):
            payload = json.dumps({"id": i, "value": f"message_{i}"})
            result = client.publish(topic, payload, qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Failed to publish message {i}: {result.rc}", file=sys.stderr)
                sys.exit(1)

        # Wait for all messages to be published
        time.sleep(2)

        client.loop_stop()
        client.disconnect()
        print(f"Successfully published {count} messages to topic '{topic}'")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python rabbitmq_operation.py produce <topic> [count]", file=sys.stderr)
        sys.exit(1)

    command = sys.argv[1]
    if command != "produce":
        print(f"Error: Unknown command '{command}'. Only 'produce' is supported.", file=sys.stderr)
        sys.exit(1)

    topic = sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    produce_messages(topic, count)
