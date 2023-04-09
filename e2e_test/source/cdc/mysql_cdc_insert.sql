USE mydb;

-- Insert new records
INSERT INTO products
VALUES (default,"RisingWave","Next generation Streaming Database"),
       (default,"Materialize","The Streaming Database You Already Know How to Use");

INSERT INTO orders
VALUES (default, '2022-12-01 15:08:22', 'Sam', 1000.52, 110, false);

UPDATE mytable SET v3 = 'yes' where v1 = 2;
DELETE FROM mytable where v1 = 1;
