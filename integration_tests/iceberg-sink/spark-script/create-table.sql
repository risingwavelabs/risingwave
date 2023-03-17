drop table if exists demo.demo_db.demo_table;

CREATE TABLE demo.demo_db.demo_table
(
  user_id string,
  target_id string,
  event_timestamp string
) TBLPROPERTIES ('format-version'='2');



