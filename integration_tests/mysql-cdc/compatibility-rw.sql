CREATE TABLE IF NOT EXISTS mysql_all_types(
c_boolean boolean,
c_bit boolean,
c_tinyint smallint,
c_smallint smallint,
c_mediumint integer,
c_integer integer,
c_bigint bigint,
c_decimal decimal,
c_float real,
c_double double,
c_char_255 varchar,
c_varchar_10000 varchar,
c_binary_255 bytea,
c_varbinary_10000 bytea,
c_date date,
c_time time,
c_timestamp timestamptz,
PRIMARY KEY (c_boolean,c_bigint,c_date)
) WITH (
connector = 'mysql-cdc',
hostname = 'mysql',
port = 3306,
username = 'root',
password = '123456',
database.name = 'mydb',
table.name = 'mysql_all_types',
server.id=5888
);
