-- SQL Server CDC timestamp test preparation
-- This file prepares test data for timestamp handling in SQL Server CDC

USE mydb;
GO

-- Enable CDC on database first
EXEC sys.sp_cdc_enable_db;
GO

-- Create table for timestamp testing
CREATE TABLE timestamp_test (
    id INT PRIMARY KEY,
    timestamp DATETIME2(7),
    time_col TIME(7)
);
GO

-- Enable CDC on the table
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'timestamp_test', @role_name = NULL;
GO

-- Extreme timestamps
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (1, '9999-01-01 00:00:00.000', '06:00:00.0000000');
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (2, '0001-01-01 00:00:00.000', '00:00:00.0000000');
INSERT INTO timestamp_test (id, timestamp, time_col) VALUES (3, '1972-01-01 00:00:00.000', '12:34:56.1234567');
GO