SET NOCOUNT ON;

EXEC sys.sp_cdc_enable_db;

CREATE TABLE quoted_col_tbl (
  id INT PRIMARY KEY,
  [a"b] INT,
  note VARCHAR(50)
);

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name = 'quoted_col_tbl',
  @role_name = NULL;

INSERT INTO quoted_col_tbl VALUES
  (1, 10, 'backfill_1'),
  (2, 20, 'backfill_2');
