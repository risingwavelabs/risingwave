# Sink Changes from RisingWave Tables to Apache Pinot

The demo was modified from the `pinot-upsert` project of https://github.com/dunithd/edu-samples

## Run the demo

1. Start the cluster with `docker compose up -d` command.
   The command will start a RisingWave cluster together with a pinot cluster with 1 controller, 1 broker and 1 server.
2. Create a kafka topic named `orders.upsert.log` for data to sink to.
```shell
docker compose exec kafka \
kafka-topics --create --topic orders.upsert.log --bootstrap-server localhost:9092
```
3. Connect the RisingWave frontend via the psql client. Create RisingWave table and sink.
```shell
psql -h localhost -p 4566 -d dev -U root

# within the psql client
dev=> CREATE TABLE IF NOT EXISTS orders
(
    id INT PRIMARY KEY,
    user_id BIGINT,
    product_id BIGINT,
    status VARCHAR,
    quantity INT,
    total FLOAT,
    created_at BIGINT,
    updated_at BIGINT
);
CREATE_TABLE
dev=> CREATE SINK orders_sink FROM orders WITH (
    connector = 'kafka',
    properties.bootstrap.server = 'kafka:9092',
    topic = 'orders.upsert.log',
    type = 'upsert',
    primary_key = 'id'
);
CREATE_SINK
```
4. Create a pinot table named `orders` that ingests data from the kafka topic
```shell
docker exec -it pinot-controller /opt/pinot/bin/pinot-admin.sh AddTable \
-tableConfigFile /config/orders_table.json \
-schemaFile /config/orders_schema.json -exec
```
5. Connector to RisingWave frontend and insert test data
```shell
psql -h localhost -p 4566 -d dev -U root

# Within the psql client
insert into orders values (1, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
insert into orders values (2, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
insert into orders values (3, 10, 100, 'INIT', 1, 1.0, 1685421033000, 1685421033000);
flush;
```
After inserting the data, query the pinot table with pinot cli
```shell
docker compose exec pinot-controller \
/opt/pinot/bin/pinot-admin.sh PostQuery -brokerHost \
pinot-broker -brokerPort 8099 -query "SELECT * FROM orders"

# Result like
{
    "rows": [
        [
            1,
            100,
            1,
            "INIT",
            1.0,
            1685421033000,
            10
        ],
        [
            2,
            100,
            1,
            "INIT",
            1.0,
            1685421033000,
            10
        ],
        [
            3,
            100,
            1,
            "INIT",
            1.0,
            1685421033000,
            10
        ]
    ]
}
```
6. Update the `status` of order with `id` as 1 to `PROCESSING`
```shell
psql -h localhost -p 4566 -d dev -U root

# Within the psql client
update orders  set status = 'PROCESSING' where id = 1;
flush;
```
After updating the data, query the pinot table with pinot cli
```shell
docker compose exec pinot-controller \
/opt/pinot/bin/pinot-admin.sh PostQuery -brokerHost \
pinot-broker -brokerPort 8099 -query "SELECT * FROM orders"

# Result like
{
    "rows": [
        [
            2,
            100,
            1,
            "INIT",
            1.0,
            1685421033000,
            10
        ],
        [
            3,
            100,
            1,
            "INIT",
            1.0,
            1685421033000,
            10
        ],
        [
            1,
            100,
            1,
            "PROCESSING",
            1.0,
            1685421033000,
            10
        ]
    ]
}
```
From the query result, we can see that the update on RisingWave table 
has been reflected on the pinot table.


By now, the demo has finished. 

## Kafka Payload Format

In the demo, there will be 4 upsert events in the kafka topic. 
The payload is like the following:
```json
{"created_at":1685421033000,"id":1,"product_id":100,"quantity":1,"status":"INIT","total":1.0,"updated_at":1685421033000,"user_id":10}
{"created_at":1685421033000,"id":2,"product_id":100,"quantity":1,"status":"INIT","total":1.0,"updated_at":1685421033000,"user_id":10}
{"created_at":1685421033000,"id":3,"product_id":100,"quantity":1,"status":"INIT","total":1.0,"updated_at":1685421033000,"user_id":10}
{"created_at":1685421033000,"id":1,"product_id":100,"quantity":1,"status":"PROCESSING","total":1.0,"updated_at":1685421033000,"user_id":10}
```
