SET NOCOUNT ON;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE [t_1] (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE [t$1] (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE [t#1] (id INT PRIMARY KEY, v VARCHAR(20));
CREATE TABLE [t中1] (id INT PRIMARY KEY, v VARCHAR(20));

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N't_1', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N't$1', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N't#1', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = N't中1', @role_name = NULL;

INSERT INTO [t_1] VALUES (10, 'snapshot');
INSERT INTO [t$1] VALUES (20, 'snapshot');
INSERT INTO [t#1] VALUES (30, 'snapshot');
INSERT INTO [t中1] VALUES (40, 'snapshot');
