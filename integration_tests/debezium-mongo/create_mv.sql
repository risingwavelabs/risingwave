CREATE MATERIALIZED VIEW normalized_users AS
SELECT
    payload ->> 'name' as name,
    payload ->> 'email' as email,
    payload ->> 'address' as address
FROM
    users;