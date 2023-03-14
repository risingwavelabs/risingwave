-- mysql -p123456 -uroot -h 127.0.0.1 mydb < mysql_prepare.sql
--
-- Mysql
USE mydb;

CREATE TABLE user_behaviors (
  user_id VARCHAR(60),
  target_id VARCHAR(60),
  target_type VARCHAR(60),
  event_timestamp VARCHAR(100),
  behavior_type VARCHAR(60),
  parent_target_type VARCHAR(60),
  parent_target_id VARCHAR(60),
  PRIMARY KEY(user_id, target_id, event_timestamp)
);
