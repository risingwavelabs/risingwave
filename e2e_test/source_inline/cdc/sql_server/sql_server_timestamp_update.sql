-- SQL Server CDC timestamp test update data
-- This file updates test data to verify CDC update functionality

USE mydb;
GO

-- Update data in SQL Server to test CDC update functionality
UPDATE timestamp_test SET id = 11 WHERE id = 1;
UPDATE timestamp_test SET id = 12 WHERE id = 2;
UPDATE timestamp_test SET id = 13 WHERE id = 3;
GO