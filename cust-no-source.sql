-- NOTE: For some reason this has UploadTask of size 44.
CREATE TABLE source_kafka (
 timestamp timestamp,
 user_id integer,
 page_id integer,
 action varchar
);

INSERT INTO source_kafka VALUES ('2023-07-28 07:11:00', 1, 1, 'gtrgretrg');
INSERT INTO source_kafka VALUES ('2023-07-28 07:11:00', 2, 1, 'fsdfgerrg');
INSERT INTO source_kafka VALUES ('2023-07-28 07:11:00', 3, 1, 'sdfergtth');

INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 4, 2, 'erwerhghj');
INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 5, 2, 'kiku7ikkk');

INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 6, 3, '6786745ge');
INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 7, 3, 'fgbgfnyyy');

INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 8, 4, 'werwerwwe');
INSERT INTO source_kafka VALUES ('2023-07-28 06:54:00', 9, 4, 'yjtyjtyyy');

CREATE TABLE source_postgres (
 obj_id integer,
 name varchar,
 age integer,
 PRIMARY KEY ( obj_id )
);

INSERT INTO source_postgres VALUES (1, '张三', 11);
INSERT INTO source_postgres VALUES (2, '李四', 22);
INSERT INTO source_postgres VALUES (3, '王五', 33);
INSERT INTO source_postgres VALUES (4, '赵六', 12);
INSERT INTO source_postgres VALUES (5, '哈哈', 34);

CREATE MATERIALIZED VIEW view_kafka_join_postgres AS 
SELECT page_id, 
timestamp as timestamp,
action as action,
user_id as user_id,
obj_id AS obj_id, 
name AS name,
age as age
FROM source_kafka join source_postgres on source_kafka.page_id=source_postgres.obj_id 
where (timestamp - INTERVAL '1 minute') > now();
