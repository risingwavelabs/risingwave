CREATE MATERIALIZED VIEW normalized_users AS
SELECT
    name as name,
    email as email,
    address as address,
    last_login as last_login
FROM
    users;
