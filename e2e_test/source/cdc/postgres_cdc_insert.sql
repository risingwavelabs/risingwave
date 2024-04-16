SELECT pg_current_wal_lsn();

INSERT INTO shipments
VALUES (default,10004,'Beijing','Shanghai',false);

INSERT INTO person VALUES (1203, '张三', 'kedmrpz@xiauh.com', '5536 1959 5460 2096', '北京');
INSERT INTO person VALUES (1204, '李四', 'egpemle@lrhcg.com', '0052 8113 1582 4430', '上海');

insert into abs.t1 values (2, 2.2, 'bbb', '1234.5431');

SELECT pg_current_wal_lsn();
select * from pg_publication_tables where pubname='rw_publication';
select * from public.person order by id;

insert into numeric_table values(102, 57896044618658097711785492504343953926634992332820282019728792003956564819967);
--- 2^255
insert into numeric_table values(103, 57896044618658097711785492504343953926634992332820282019728792003956564819968);
--- 2^256
insert into numeric_table values(104, 115792089237316195423570985008687907853269984665640564039457584007913129639936);
insert into numeric_table values(105, 115792089237316195423570985008687907853269984665640564039457584007913129639936.555555);
