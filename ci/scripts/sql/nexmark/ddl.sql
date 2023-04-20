CREATE SOURCE nexmark (
event_type BIGINT,
person STRUCT<"id" BIGINT,
	    "name" VARCHAR,
	    "email_address" VARCHAR,
	    "credit_card" VARCHAR,
	    "city" VARCHAR,
	    "state" VARCHAR,
	    "date_time" TIMESTAMP,
	    "extra" VARCHAR>,
auction STRUCT<"id" BIGINT,
	    "item_name" VARCHAR,
	    "description" VARCHAR,
	    "initial_bid" BIGINT,
	    "reserve" BIGINT,
	    "date_time" TIMESTAMP,
	    "expires" TIMESTAMP,
	    "seller" BIGINT,
	    "category" BIGINT,
	    "extra" VARCHAR>,
bid STRUCT<"auction" BIGINT,
	"bidder" BIGINT,
	"price" BIGINT,
	"channel" VARCHAR,
	"url" VARCHAR,
	"date_time" TIMESTAMP,
	"extra" VARCHAR>
) WITH (
connector = 'kafka',
topic = 'nexmark',
properties.bootstrap.server = 'localhost:9092',
kafka.scan.startup.mode = 'earliest'
) ROW FORMAT JSON;

CREATE VIEW bid
AS
SELECT (bid).* FROM nexmark WHERE event_type = 2;
CREATE VIEW auction
AS
SELECT (auction).* FROM nexmark WHERE event_type = 1;

CREATE VIEW person
AS
SELECT (person).* FROM nexmark WHERE event_type = 0;