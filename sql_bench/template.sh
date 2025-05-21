#!/usr/bin/env bash

##################### Common

run_psql() {
  psql "$PSQL_URL" "$@"
}

export -f run_psql

prepare() {
  bench_prepare="
    SET RW_IMPLICIT_FLUSH=true;
    CREATE TABLE users (
      id INTEGER,
      name VARCHAR,
      PRIMARY KEY (id)
    );

    CREATE TABLE orders (
      id INTEGER,
      user_id INTEGER,
      amount INTEGER,
      PRIMARY KEY (id)
    ) WITH (
      connector = 'datagen',
      datagen.rows.per.second = 1000,

      fields.id.kind = 'sequence',
      fields.id.start = '1',
      fields.id.end = '10000',

      fields.user_id.kind = 'random',
      fields.user_id.min = '1',
      fields.user_id.max = '1000',
      fields.user_id.seed = '1',

      fields.amount.kind = 'random',
      fields.amount.min = '10',
      fields.amount.max = '1000',
      fields.amount.seed = '2'
    );

    -- Insert static data for users
    INSERT INTO users 
    SELECT t.id as id, 'user_' || t.id::text
    FROM generate_series(1, 1000) t(id);
  "
  run_psql -c "${bench_prepare}"

  # Wait for orders to generate some data
  sleep 5

  # Create a snapshot of orders
  run_psql -c "
    CREATE TABLE orders_snapshot AS SELECT * FROM orders;
    DROP TABLE orders;
    ALTER TABLE orders_snapshot RENAME TO orders;
  "
}

conclude() {
  bench_teardown="
    drop materialized view if exists m1;
    drop table if exists orders;
    drop table if exists users;
  "
  run_psql -c "${bench_teardown}"
}

export -f prepare
export -f conclude

##################### Baseline

baseline_benchmark() {
  bench_workload="
    create materialized view m1
      with (backfill_order = NONE)
      as
        select
          users.name,
          count(*) as order_count,
          sum(amount) as total_amount,
          avg(amount) as avg_amount,
          min(amount) as min_amount,
          max(amount) as max_amount
        from orders
          left join users on orders.user_id = users.id
        group by users.name;
  "
  run_psql -c "${bench_workload}"
}

export -f baseline_benchmark

##################### Optimized

optimized_benchmark() {
  bench_workload="
    create materialized view m1
      with (backfill_order = FIXED(users -> orders))
      as
        select
          users.name,
          count(*) as order_count,
          sum(amount) as total_amount,
          avg(amount) as avg_amount,
          min(amount) as min_amount,
          max(amount) as max_amount
        from orders
          left join users on orders.user_id = users.id
        group by users.name;
  "
  run_psql -c "${bench_workload}"
}

export -f optimized_benchmark

hyperfine --shell=bash --runs 1 --show-output \
  --prepare 'conclude; prepare' --conclude 'conclude' baseline_benchmark \
  --prepare 'conclude; prepare' --conclude 'conclude' optimized_benchmark 