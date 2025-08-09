-- SQL Server CDC timestamp test insert data
-- This file inserts additional test data during the test process

USE mydb;
GO

-- Insert additional test data (3 rows for update testing)
INSERT INTO timestamp_test (id, timestamp) VALUES (4, '8000-06-01 12:00:00.000');
INSERT INTO timestamp_test (id, timestamp) VALUES (5, '5138-11-16 09:46:40.000');
INSERT INTO timestamp_test (id, timestamp) VALUES (6, '9999-12-31 23:59:59.999999');
GO