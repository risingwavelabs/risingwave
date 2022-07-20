CREATE TABLE person (
    id BIGINT,
    name VARCHAR,
    email_address VARCHAR,
    credit_card VARCHAR,
    city VARCHAR,
    state VARCHAR,
    date_time TIMESTAMP
);

CREATE TABLE auction (
    id BIGINT,
    item_name VARCHAR,
    description VARCHAR,
    initial_bid BIGINT,
    reserve BIGINT,
    date_time TIMESTAMP,
    expires TIMESTAMP,
    seller BIGINT,
    category BIGINT
);

CREATE TABLE bid (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    date_time TIMESTAMP
);
