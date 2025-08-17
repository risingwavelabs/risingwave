-- SQL Server CDC timestamp test insert data
-- This file inserts additional test data during the test process

USE mydb;
GO

-- Insert additional test data (3 rows for update testing)
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (4, '8000-06-01 12:00:00.000', '12:00:00.0000000');
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (5, '5138-11-16 09:46:40.000', '09:46:40.0000000');
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (6, '9999-12-31 23:59:59.999999', '23:00:00.0000000');
GO