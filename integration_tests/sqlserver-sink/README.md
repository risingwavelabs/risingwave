# Demo: Sinking to Microsoft SQL Server

In this demo, we want to showcase how RisingWave is able to sink data to Microsoft SQL Server.


1. Launch the cluster:

```sh
docker-compose up -d
```

The cluster contains a RisingWave cluster and its necessary dependencies, a datagen that generates the data, a SQL Server instance for sink.

2. Create the SQL Server table:

```sh
docker exec -it sqlserver-server /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P SomeTestOnly@SA -Q "
CREATE DATABASE SinkTest;
GO
USE SinkTest;
GO
CREATE TABLE t_many_data_type (
    k1 int, k2 int,
    c_boolean bit,
    c_int16 smallint,
    c_int32 int,
    c_int64 bigint,
    c_float32 float,
    c_float64 float,
    c_decimal decimal,
    c_date date,
    c_time time,
    c_timestamp datetime2,
    c_nvarchar nvarchar(1024),
    c_varbinary varbinary(1024),
    PRIMARY KEY (k1,k2)
);
GO"
```

3. Create the RisingWave table and sink:

```sh
docker exec -it postgres-0 psql -h 127.0.0.1 -p 4566 -d dev -U root -c "
CREATE TABLE t_many_data_type_rw (
    k1 int, k2 int,
    c_int16 smallint,
    c_int32 int,
    c_int64 bigint,
    c_float32 float,
    c_float64 double,
    c_timestamp timestamp,
    c_nvarchar string
) WITH (
    connector = 'datagen',
    datagen.split.num = '1',
    datagen.rows.per.second = '100',
    fields.k1.kind = 'random',
    fields.k1.min = '0',
    fields.k1.max = '10000',
    fields.k2.kind = 'random',
    fields.k2.min = '0',
    fields.k2.max = '10000'
);

CREATE SINK s_many_data_type FROM t_many_data_type_rw WITH (
  connector = 'sqlserver',
  type = 'upsert',
  sqlserver.host = 'localhost',
  sqlserver.port = 1433,
  sqlserver.user = 'SA',
  sqlserver.password = 'SomeTestOnly@SA',
  sqlserver.database = 'SinkTest',
  sqlserver.table = 't_many_data_type',
  primary_key = 'k1,k2',
);
"
```

4. Verify the result in SQL Server, for example:

```sh
docker exec -it sqlserver-server /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P SomeTestOnly@SA -Q "
SELECT count(*) FROM SinkTest.dbo.t_many_data_type;
"
```
