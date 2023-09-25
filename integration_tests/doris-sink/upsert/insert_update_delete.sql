INSERT INTO user_behaviors VALUES(1,'1','1','2020-01-01 01:01:01','1','1','1'),
(2,'2','2','2020-01-01 01:01:02','2','2','2'),
(3,'3','3','2020-01-01 01:01:03','3','3','3'),
(4,'4','4','2020-01-01 01:01:04','4','4','4');

DELETE FROM user_behaviors WHERE user_id = 2;

UPDATE user_behaviors SET target_id = 30 WHERE user_id = 3;