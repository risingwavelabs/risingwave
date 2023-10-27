import sys
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema


def main():
    url = sys.argv[1]
    subject = sys.argv[2]
    with open(sys.argv[3]) as f:
        schema_str = f.read()
    if 4 < len(sys.argv):
        keys = sys.argv[4].split(',')
    else:
        keys = []

    client = SchemaRegistryClient({"url": url})

    if keys:
        schema_str = select_keys(schema_str, keys)
    else:
        schema_str = remove_unsupported(schema_str)
    schema = Schema(schema_str, 'AVRO')
    client.register_schema(subject, schema)


def select_fields(schema_str, f):
    import json
    root = json.loads(schema_str)
    if not isinstance(root, dict):
        return schema_str
    if root['type'] != 'record':
        return schema_str
    root['fields'] = f(root['fields'])
    return json.dumps(root)


def remove_unsupported(schema_str):
    return select_fields(schema_str, lambda fields: [f for f in fields if f['name'] not in {'unsupported', 'mon_day_sec_field'}])


def select_keys(schema_str, keys):
    def process(fields):
        by_name = {f['name']: f for f in fields}
        return [by_name[k] for k in keys]
    return select_fields(schema_str, process)


if __name__ == '__main__':
    main()
