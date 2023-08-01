create table test(obj_id integer, name varchar, age integer);

CREATE SINK test_sink from test WITH (
    connector='jdbc',
    jdbc.url='jdbc:postgresql://localhost:5432/postgres?user=postgres&password=abc',
    table.name='ceshi2',
    type='upsert'
);

insert into test values (1, '123', 1);