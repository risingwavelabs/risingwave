CREATE TABLE personnel (
    id integer,
    name varchar,
);

CREATE SINK s_nats FROM personnel WITH (
    connector='nats',
    server_url='nats-server:4222',
    subject='subject1',
    type='append-only',
    force_append_only='true',
    connect_mode='plain'
);

INSERT INTO personnel VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO personnel VALUES (3, 'Tom'), (4, 'Jerry');

FLUSH;
