CREATE SOURCE ad_impression (
    bid_id BIGINT,
    ad_id BIGINT,
    impression_timestamp TIMESTAMPTZ
) WITH (
    connector = 'kinesis',
    stream = 'ad-impression',
    aws.region='us-east-1',
    scan.startup.mode='earliest',
    aws.credentials.access_key_id ='test',
    aws.credentials.secret_access_key='test',
    endpoint='http://localstack:4566'
) FORMAT PLAIN ENCODE JSON;

CREATE SOURCE ad_click (
    bid_id BIGINT,
    click_timestamp TIMESTAMPTZ
) WITH (
    connector = 's3',
    s3.region_name = 'us-east-1',
    s3.bucket_name = 'ad-click',
    s3.credentials.access = 'test',
    s3.credentials.secret = 'test',
    s3.endpoint_url = 'http://localstack:4566'
) FORMAT PLAIN ENCODE JSON;
