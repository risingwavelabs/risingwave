CREATE TABLE user_behaviors (
    user_id VARCHAR,
    target_id VARCHAR,
    target_type VARCHAR,
    event_timestamp VARCHAR,
    behavior_type VARCHAR,
    parent_target_type VARCHAR,
    parent_target_id VARCHAR,
    PRIMARY KEY(user_id, target_id, event_timestamp)
) with (
    connector = 'mysql-cdc',
    hostname = 'mysql',
    port = '3306',
    username = 'root',
    password = '123456',
    database.name = 'mydb',
    table.name = 'user_behaviors',
    server.id = '1'
);