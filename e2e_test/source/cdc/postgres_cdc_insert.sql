SELECT pg_current_wal_lsn();

INSERT INTO shipments
VALUES (default,10004,'Beijing','Shanghai',false);

INSERT INTO person VALUES (1203, '张三', 'kedmrpz@xiauh.com', '5536 1959 5460 2096', '北京');
INSERT INTO person VALUES (1204, '李四', 'egpemle@lrhcg.com', '0052 8113 1582 4430', '上海');

insert into abs.t1 values (2, 2.2, 'bbb', '1234.5431');

SELECT pg_current_wal_lsn();
select * from pg_publication_tables where pubname='rw_publication';
select * from public.person order by id;
