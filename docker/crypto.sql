CREATE TABLE erc20_transfers (
  address VARCHAR,
  amount BIGINT,
  block_number INT,
  chain BIGINT,
  from_address VARCHAR,
  log_index SMALLINT,
  log_type VARCHAR,
  timestamp TIMESTAMP,
  to_address VARCHAR,
  token_address VARCHAR,
  transaction_hash VARCHAR,
  transaction_log_index SMALLINT
)
WITH (
  connector = 'datagen',

  -- Configure string columns with random strings of length 42 (typical Ethereum address length)
  fields.address.kind = 'random',
  fields.address.length = '42',

  fields.from_address.kind = 'random',
  fields.from_address.length = '42',

  fields.to_address.kind = 'random',
  fields.to_address.length = '42',

  fields.token_address.kind = 'random',
  fields.token_address.length = '42',

  fields.transaction_hash.kind = 'random',
  fields.transaction_hash.length = '66',  -- typical tx hash length (0x + 64 hex chars)

  fields.log_type.kind = 'random',
  fields.log_type.length = '10',

  -- Numeric columns with ranges
  fields.amount.kind = 'random',
  fields.amount.min = '0',
  fields.amount.max = '1000000000',

  fields.block_number.kind = 'sequence',
  fields.block_number.start = '100000000',
  fields.block_number.end = '110000000',

  fields.chain.kind = 'random',
  fields.chain.min = '1',
  fields.chain.max = '1',

  fields.log_index.kind = 'random',
  fields.log_index.start = '0',
  fields.log_index.end = '100',

  fields.transaction_log_index.kind = 'random',
  fields.transaction_log_index.start = '0',
  fields.transaction_log_index.end = '100',

  fields.timestamp.kind = 'random',
  fields.timestamp.max_past = '24h',
  fields.timestamp.max_past_mode = 'relative',

  datagen.rows.per.second = '1000000000'
)
FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW count_erc20_transfers as
select count(*) from erc20_transfers;


CREATE SINK sink1 FROM erc20_transfers
WITH (
   connector='kafka',
   properties.bootstrap.server='message_queue:29092',
   topic='crypto'
)
FORMAT PLAIN ENCODE JSON (
   force_append_only='true'
);


CREATE TABLE erc20_transfers_source (
  address VARCHAR,
  amount BIGINT,
  block_number INT,
  chain BIGINT,
  from_address VARCHAR,
  log_index SMALLINT,
  log_type VARCHAR,
  timestamp TIMESTAMP,
  to_address VARCHAR,
  token_address VARCHAR,
  transaction_hash VARCHAR,
  transaction_log_index SMALLINT
) with (
    connector='kafka',
    topic='crypto',
    properties.bootstrap.server='message_queue:29092'
) FORMAT PLAIN ENCODE JSON;

CREATE MATERIALIZED VIEW IF NOT EXISTS token_holders_current AS
WITH balance_changes AS (
    -- Outgoing transfers (negative balance change)
    SELECT
        erc20 as token_address,
        from_address as holder_address,
        -CAST(value AS NUMERIC) as balance_change,
        block_number,
        transaction_hash,
        chain_id
    FROM erc20_transfers
    WHERE from_address != '0x0000000000000000000000000000000000000000'
        AND chain_id = {}

    UNION ALL

    -- Incoming transfers (positive balance change)
    SELECT
        erc20 as token_address,
        to_address as holder_address,
        CAST(value AS NUMERIC) as balance_change,
        block_number,
        transaction_hash,
        chain_id
    FROM erc20_transfers
    WHERE to_address != '0x0000000000000000000000000000000000000000'
        AND chain_id = {}
),
holder_balances AS (
    SELECT
        token_address,
        holder_address,
        SUM(balance_change) as current_balance,
        MIN(block_number) as first_seen_block,
        MAX(block_number) as last_updated_block,
        chain_id
    FROM balance_changes
    GROUP BY token_address, holder_address, chain_id
)
SELECT
    token_address,
    holder_address,
    current_balance,
    first_seen_block,
    last_updated_block,
    chain_id
FROM holder_balances
WHERE current_balance > 0;

CREATE MATERIALIZED VIEW count_erc20_transfers_source as
select count(*) from erc20_transfers_source;
