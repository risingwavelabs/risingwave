CREATE TABLE person (
    id BIGINT,
    name VARCHAR,
    emailAddress VARCHAR,
    creditCard VARCHAR,
    city VARCHAR,
    state VARCHAR,
    dateTime TIMESTAMP
);

CREATE TABLE auction (
    id BIGINT,
    itemName VARCHAR,
    description VARCHAR,
    initialBid BIGINT,
    reserve BIGINT,
    dateTime TIMESTAMP,
    expires TIMESTAMP,
    seller BIGINT,
    category BIGINT
);

CREATE TABLE bid (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    dateTime TIMESTAMP
);
