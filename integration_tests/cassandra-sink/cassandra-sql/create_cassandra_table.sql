CREATE KEYSPACE my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use my_keyspace;
CREATE table demo_test(
    user_id int primary key,
    target_id text,
    event_timestamp timestamp,
);