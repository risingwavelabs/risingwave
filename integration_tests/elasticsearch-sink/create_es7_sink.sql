CREATE SINK bhv_es_sink
FROM
    bhv_mv WITH (
    connector = 'elasticsearch',
    index = 'test',
    url = 'http://elasticsearch8:9200',
    username = 'elastic',
    password = 'risingwave'
);