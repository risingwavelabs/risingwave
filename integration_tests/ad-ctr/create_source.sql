CREATE SOURCE ad_impression (
    bid_id BIGINT,
    ad_id BIGINT,
    impression_timestamp TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'ad_impression',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE SOURCE ad_click (
    bid_id BIGINT,
    click_timestamp TIMESTAMPTZ
) WITH (
    connector = 'kafka',
    topic = 'ad_click',
    properties.bootstrap.server = 'message_queue:29092',
    scan.startup.mode = 'earliest'
) ROW FORMAT JSON;