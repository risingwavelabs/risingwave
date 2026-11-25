SET NOCOUNT ON;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE excl_tbl (id INT PRIMARY KEY, name VARCHAR(50), payload VARCHAR(50), raw_data VARCHAR(50));
CREATE TABLE excl_tbl_pk2 (id1 INT, id2 INT, payload VARCHAR(50), PRIMARY KEY (id1, id2));

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'excl_tbl', @role_name = NULL;
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'excl_tbl_pk2', @role_name = NULL;

INSERT INTO excl_tbl VALUES (1, 'a', 'big', 'raw');
INSERT INTO excl_tbl_pk2 VALUES (1, 1, 'big');
