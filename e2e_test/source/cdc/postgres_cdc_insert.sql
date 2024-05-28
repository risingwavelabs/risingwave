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
insert into numeric_table values(106, 'NaN'::numeric);
insert into numeric_table values(107, 'Infinity'::numeric);

INSERT INTO enum_table VALUES (3, 'sad');
--- to avoid escaping issues of psql -c "", we insert this row here and check the result in check_new_rows.slt
INSERT INTO list_with_null VALUES (3, '{NULL,-3,-4}', '{NULL,nan,-inf}', '{NULL,nan,-inf}', '{NULL,nan,-inf}', '{NULL,sad,ok}', '{NULL,471acecf-a4b4-4ed3-a211-7fb2291f159f,9bc35adf-fb11-4130-944c-e7eadb96b829}', '{NULL,\\x99,\\xAA}');
INSERT INTO list_with_null VALUES (4, '{-4,-5,-6}', '{NULL,-99999999999999999.9999}', '{NULL,-99999999999999999.9999}', '{NULL,-99999999999999999.9999}', '{NULL,sad,ok}', '{b2e4636d-fa03-4ad4-bf16-029a79dca3e2}', '{\\x88,\\x99,\\xAA}');
