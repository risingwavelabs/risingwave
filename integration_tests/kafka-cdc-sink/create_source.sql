CREATE TABLE metrics (
    id int,
    num int,
) WITH (
    connector = 'datagen',
    fields.id.kind = 'random',
    fields.id.min= '1',
    fields.id.max= '10000000',
    fields.num.kind = 'random',
    fields.num.min= '-100',
    fields.num.max= '100000',
    datagen.rows.per.second = '10'
) ROW FORMAT JSON;

CREATE MATERIALIZED VIEW counts as select id, sum(num) from metrics group by id;