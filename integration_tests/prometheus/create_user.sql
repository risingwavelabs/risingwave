create user grafanareader with password 'password';

-- It is recommended to use a dedicated read-only user when querying the database using Grafana.
grant
select
    on materialized view metric_avg_30s to grafanareader;