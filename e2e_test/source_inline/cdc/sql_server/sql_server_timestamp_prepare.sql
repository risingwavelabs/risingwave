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
    timestamp DATETIME2(7)
);
GO

-- Enable CDC on the table
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'timestamp_test', @role_name = NULL;
GO

-- Extreme timestamps
INSERT INTO timestamp_test (id, timestamp) VALUES (1, '9999-01-01 00:00:00.000');
INSERT INTO timestamp_test (id, timestamp) VALUES (2, '0001-01-01 00:00:00.000');
INSERT INTO timestamp_test (id, timestamp) VALUES (3, '1972-01-01 00:00:00.000');
GO