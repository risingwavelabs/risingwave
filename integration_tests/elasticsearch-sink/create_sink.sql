CREATE SINK bhv_es7_sink
FROM
    bhv_mv WITH (
    connector = 'elasticsearch',
    index = 'test',
    url = 'http://elasticsearch7:9200',
    username = 'elastic',
    password = 'risingwave'
);

CREATE SINK bhv_es8_sink
FROM
    bhv_mv WITH (
    connector = 'elasticsearch',
    index = 'test',
    url = 'http://elasticsearch8:9200',
    username = 'elastic',
    password = 'risingwave'
);
