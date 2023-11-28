CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use demo;
CREATE table demo_bhv_table(
    user_id int primary key,
    target_id text,
    event_timestamp timestamp,
);
