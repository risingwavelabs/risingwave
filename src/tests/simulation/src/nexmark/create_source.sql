create source auction (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    date_time TIMESTAMP,
    expires TIMESTAMP,
    seller BIGINT,
    category BIGINT,
    extra VARCHAR
    {watermark_column})
with (
    connector = 'nexmark',
    nexmark.table.type = 'Auction'
    {extra_args}
);

create source bid (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel VARCHAR,
    url VARCHAR,
    date_time TIMESTAMP,
    extra VARCHAR
    {watermark_column})
with (
    connector = 'nexmark',
    nexmark.table.type = 'Bid'
    {extra_args}
);

create source person (
    id BIGINT,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP,
    extra VARCHAR
    {watermark_column})
with (
    connector = 'nexmark',
    nexmark.table.type = 'Person'
    {extra_args}
);
