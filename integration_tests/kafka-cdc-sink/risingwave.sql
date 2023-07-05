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


CREATE SINK IF NOT EXISTS counts_sink
FROM counts
WITH (
connector = 'kafka',
properties.bootstrap.server='message_queue:29092',
topic = 'counts',
type = 'debezium',
use_transaction = 'false',
primary_key = 'id'
);