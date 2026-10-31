SET NOCOUNT ON;

EXEC sys.sp_cdc_enable_db;

EXEC('CREATE SCHEMA a');

CREATE TABLE dbo.[a.b] (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE a.b (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE dbo.plain (id INT PRIMARY KEY, v VARCHAR(20));

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N'a.b', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'a', @source_name = N'b', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N'plain', @role_name = NULL;
