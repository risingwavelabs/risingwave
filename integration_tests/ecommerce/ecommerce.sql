CREATE SOURCE order_events (
    order_id VARCHAR,
    item_id VARCHAR,
    item_price DOUBLE PRECISION,
    event_timestamp TIMESTAMP
) WITH (
    connector = 'kafka',
    topic = 'nics_metrics',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE TABLE order_events (
    order_id VARCHAR,
    item_id VARCHAR,
    item_price DOUBLE PRECISION,
    event_timestamp TIMESTAMP
);

CREATE TABLE parcel_events (
    order_id VARCHAR,
    event_timestamp TIMESTAMP,
    event_type VARCHAR
);

CREATE MATERIALIZED VIEW order_details AS
SELECT
    order_id,
    SUM(item_price) AS total_price,
    AVG(item_price) AS avg_price
FROM
    order_events
GROUP BY
    order_id;

CREATE MATERIALIZED VIEW order_details AS
SELECT
    t1.order_id AS order_id,
    (t2.event_timestamp - t1.event_timestamp) as delivery_time
FROM
    (
        SELECT
            order_id,
            event_timestamp
        FROM
            parcel_events
        WHERE
            event_type = 'order_created'
    ) AS t1
    JOIN (
        SELECT
            order_id,
            event_timestamp
        FROM
            parcel_events
        WHERE
            event_type = 'parcel_shipped'
    ) t2 ON t1.order_id = t2.order_id
WHERE
    t2.event_timestamp - t1.event_timestamp > INTERVAL '7 days';
