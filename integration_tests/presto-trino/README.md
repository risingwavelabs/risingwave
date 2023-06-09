# Querying RisingWave from presto/trino PostgreSQL connector

## Run the demo

1. Start the cluster with `docker compose up -d` command. 
The command will start a RisingWave cluster together with a integrated trino and presto instance.
2. Connect the RisingWave frontend via the psql client. Create and insert data into the RisingWave table.
```shell
psql -h localhost -p 4566 -d dev -U root

# within the psql client
dev=> create table test_table(id bigint);
CREATE_TABLE
dev=> insert into test_table values(1);
INSERT 0 1
dev=> flush;
FLUSH
```
3. Query RisingWave from presto or trino
```shell
# Start trino client
docker compose run trino-client
# Or start presto client
docker compose run presto-client

# within the trino/presto client
trino:public> show tables;
   Table    
------------
 test_table 
(1 row)

trino:public> select * from test_table;
 id 
----
  1 
(1 row)
```