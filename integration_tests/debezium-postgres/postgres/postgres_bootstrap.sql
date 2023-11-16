-- https://www.postgresql.org/docs/current/logical-replication.html
ALTER SYSTEM SET wal_level = logical;

ALTER ROLE postgresuser WITH REPLICATION;
