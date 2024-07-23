-- please note that the column name(s) for your mv should be *exactly*
-- the same as the column name(s) in your snowflake table, since we are matching column by name.

CREATE MATERIALIZED VIEW ss_mv AS
WITH sub AS changelog FROM user_behaviors
SELECT
    user_id,
    target_id,
    event_timestamp AT TIME ZONE 'America/Indiana/Indianapolis' as event_timestamp,
    changelog_op AS __op,
    _changelog_row_id::bigint AS __row_id
FROM
    sub;