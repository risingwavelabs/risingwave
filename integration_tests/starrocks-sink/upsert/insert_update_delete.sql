INSERT INTO user_behaviors VALUES(1,'1','1','2020-01-01T01:01:01Z','1','1','1'),
(2,'2','2','2020-01-01T01:01:02Z','2','2','2'),
(3,'3','3','2020-01-01T01:01:03Z','3','3','3'),
(4,'4','4','2020-01-01T01:01:04Z','4','4','4');

DELETE FROM user_behaviors WHERE user_id = 2;

UPDATE user_behaviors SET target_id = 30 WHERE user_id = 3;
