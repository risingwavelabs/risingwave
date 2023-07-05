-- impression_timestamp: The time when the ad was shown.
-- click_timestamp: The time when the ad was clicked.
create source ad_source (
    user_id bigint,
    ad_id bigint,
    click_timestamp timestamptz,
    impression_timestamp timestamptz
) with (
    connector = 'kafka',
    topic = 'ad_clicks',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) row format json;