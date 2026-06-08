import json
import socket
import sys
import time
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


def encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("value must be non-negative")

    output = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            output.append(byte | 0x80)
        else:
            output.append(byte)
            break
    return bytes(output)


def encode_protobuf_message(record_id: int, value: str) -> bytes:
    value_bytes = value.encode("utf-8")

    # field 1: id (int32), tag = 1 << 3 | 0
    payload = bytearray(b"\x08")
    payload.extend(encode_varint(record_id))

    # field 2: value (string), tag = 2 << 3 | 2
    payload.extend(b"\x12")
    payload.extend(encode_varint(len(value_bytes)))
    payload.extend(value_bytes)
    return bytes(payload)


def build_payload(index: int, message_format: str):
    if message_format == "json":
        return json.dumps({"id": index, "value": f"message_{index}"})
    if message_format == "protobuf":
        return encode_protobuf_message(index, f"message_{index}")
    raise ValueError(f"Unsupported format: {message_format}")


def produce_messages(topic: str, count: int, message_format: str):
    """Produce messages to RabbitMQ via MQTT."""
    # Check if host is reachable
    try:
        socket.gethostbyname(RABBITMQ_HOST)
    except socket.gaierror as e:
        print(f"Error: Cannot resolve hostname '{RABBITMQ_HOST}': {e}", file=sys.stderr)
        sys.exit(1)

    client = mqtt.Client(
        client_id=f"rabbitmq_producer_{int(time.time())}",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5,
    )
    client.on_connect = on_connect

    try:
        print(f"Connecting to {RABBITMQ_HOST}:{RABBITMQ_PORT}...")
        client.connect(RABBITMQ_HOST, RABBITMQ_PORT, 60)
        client.loop_start()

        # Wait for connection to establish
        time.sleep(1)

        print(f"Publishing {count} {message_format} messages to topic '{topic}'...")
        for i in range(count):
            payload = build_payload(i, message_format)
            result = client.publish(topic, payload, qos=1)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Failed to publish message {i}: {result.rc}", file=sys.stderr)
                sys.exit(1)

        # Wait for all messages to be published
        time.sleep(2)

        client.loop_stop()
        client.disconnect()
        print(f"Successfully published {count} {message_format} messages to topic '{topic}'")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Usage: python rabbitmq_operation.py produce <topic> [count] [json|protobuf]",
            file=sys.stderr,
        )
        sys.exit(1)

    command = sys.argv[1]
    if command != "produce":
        print(f"Error: Unknown command '{command}'. Only 'produce' is supported.", file=sys.stderr)
        sys.exit(1)

    topic = sys.argv[2]
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 100
    message_format = sys.argv[4] if len(sys.argv) > 4 else "json"
    produce_messages(topic, count, message_format)
