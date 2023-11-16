create SINK orders_sink FROM orders WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'kafka:9092',
    topic = 'orders.upsert.log',
    type = 'upsert',
    primary_key = 'id'
);