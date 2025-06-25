# Demo: SInk to NATS

In this demo, we want to showcase how RisingWave is able to sink data to NATS.

1. Launch the cluster:

```sh
docker compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a clichouse for sink.


2. test sink:

```sh
// use nats to sub the data
nats sub subject1 --server=localhost:4222
// connect to risingwave and manually execute the sql file create_sink.sql
psql -h localhost -p 4566 -d dev -U root

// check the result in your nats, you should see the output:
-> nats sub subject1 --server=localhost:4222
19:15:54 Subscribing on event1
[#1] Received on "event1" with reply "_INBOX.LejntfCKRbn7rQ989qw9Bm"
{"id":2,"name":"Bob"}

[#2] Received on "event1" with reply "_INBOX.LejntfCKRbn7rQ989qw9GH"
{"id":1,"name":"Alice"}

[#3] Received on "event1" with reply "_INBOX.LejntfCKRbn7rQ989qw9Km"
{"id":4,"name":"Jerry"}

[#4] Received on "event1" with reply "_INBOX.LejntfCKRbn7rQ989qw9PH"
{"id":3,"name":"Tom"}
```


3. test source:

Connect to risingwave and manually execute the following SQL.

```sql
CREATE TABLE
  nats_source_table (id integer, name varchar)
WITH
  (
    connector = 'nats',
    server_url = 'nats-server:4222',
    subject = 'subject1',
    stream = 'my_stream',
    allow_create_stream = 'true',
    connect_mode = 'plain'
  ) FORMAT PLAIN ENCODE JSON;
```

```sh
// publish data into nats
nats pub subject2 --server=localhost:4222 --count=20 "{\"id\":{{Count}},\"name\":\"Alice{{Count}}\"}"

// check result
dev=> select * from nats_source_table order by id;
 id |  name
----+---------
  1 | Alice1
  2 | Alice2
  3 | Alice3
  4 | Alice4
  5 | Alice5
  6 | Alice6
  7 | Alice7
  8 | Alice8
  9 | Alice9
 10 | Alice10
 11 | Alice11
 12 | Alice12
 13 | Alice13
 14 | Alice14
 15 | Alice15
 16 | Alice16
 17 | Alice17
 18 | Alice18
 19 | Alice19
 20 | Alice20
(20 rows)
```



