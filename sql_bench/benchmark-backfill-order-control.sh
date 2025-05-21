#!/usr/bin/env bash

##################### Common

psql_remote() {
  psql "$PSQL_URL" "$@"
}

export -f psql_remote

# Ensure you're using release-profile via ./risedev configure
setup() {
  ./risedev k
  ./risedev clean-data
  ./risedev d full
}

cleanup() {
  ./risedev k
  ./risedev clean-data
}

prepare() {
  bench_prepare="
    SET RW_IMPLICIT_FLUSH=true;
    CREATE TABLE car_info (
      id INTEGER,
      name VARCHAR,
      PRIMARY KEY (id)
    );

    CREATE TABLE car_regions (
      id INTEGER,
      region VARCHAR,
      PRIMARY KEY (id)
    );

    CREATE TABLE car_colors (
      id INTEGER,
      color VARCHAR,
      PRIMARY KEY (id)
    );

    CREATE TABLE car_types (
      id INTEGER,
      type VARCHAR,
      PRIMARY KEY (id)
    );

    CREATE TABLE car_sales (
      id INTEGER,
      car_id INTEGER,
      region_id INTEGER,
      color_id INTEGER,
      type_id INTEGER,
      price INTEGER,
      PRIMARY KEY (id)
    ) WITH (
      connector = 'datagen',
      datagen.rows.per.second = 100000,

      fields.id.kind = 'sequence',
      fields.id.start = '1',
      fields.id.end = '1000000',

      fields.car_id.kind = 'random',
      fields.car_id.min = '119901',
      fields.car_id.max = '120000',
      fields.car_id.seed = '1',

      fields.region_id.kind = 'random',
      fields.region_id.min = '109901',
      fields.region_id.max = '110000',
      fields.region_id.seed = '10',

      fields.color_id.kind = 'random',
      fields.color_id.min = '149900',
      fields.color_id.max = '150000',
      fields.color_id.seed = '15',

      fields.type_id.kind = 'random',
      fields.type_id.min = '114901',
      fields.type_id.max = '115000',
      fields.type_id.seed = '20',

      fields.price.kind = 'random',
      fields.price.min = '1000',
      fields.price.max = '100000',
      fields.price.seed = '25'
    );

    -- Insert static data for car_info
    INSERT INTO car_info 
    SELECT t.id as id, 'car_' || t.id::text
    FROM generate_series(1, 120000) t(id);

    -- Insert static data for car_regions
    INSERT INTO car_regions 
    SELECT t.id as id, 'region_' || t.id::text
    FROM generate_series(1, 110000) t(id);

    -- Insert static data for car_colors
    INSERT INTO car_colors 
    SELECT t.id as id, 'color_' || t.id::text
    FROM generate_series(1, 150000) t(id);

    -- Insert static data for car_types
    INSERT INTO car_types
    SELECT t.id as id, 'type_' || t.id::text
    FROM generate_series(1, 115000) t(id);
  "
  psql_remote -c "${bench_prepare}"

  # Wait for car_sales to generate some data
  sleep 10

  # Create a snapshot of car_sales
  psql_remote -c "
    CREATE TABLE car_sales_snapshot AS SELECT * FROM car_sales;
    DROP TABLE car_sales;
    ALTER TABLE car_sales_snapshot RENAME TO car_sales;
  "
}

conclude() {
  bench_teardown="
    drop materialized view if exists m1;
    drop table if exists car_sales;
    drop table if exists car_info;
    drop table if exists car_regions;
    drop table if exists car_colors;
    drop table if exists car_types;
  "
  psql_remote -c "${bench_teardown}"
}

export -f setup
export -f cleanup
export -f prepare
export -f conclude

##################### Baseline

worst_benchmark() {
  bench_workload="
    create materialized view m1
      with (backfill_order = FIXED(car_sales -> car_info, car_info -> car_regions, car_regions -> car_colors, car_colors -> car_types))
      as
        with price_ranges as (
          select
            car_info.name as name,
            car_regions.region as region,
            car_colors.color as color,
            car_types.type as type,
            car_sales.price as price,
            round(ln(1 + car_sales.price)::numeric, 1) as price_range
          from car_sales
            left join car_info on car_sales.car_id = car_info.id
            left join car_regions on car_sales.region_id = car_regions.id
            left join car_colors on car_sales.color_id = car_colors.id
            left join car_types on car_sales.type_id = car_types.id
        )
        select
          name,
          region,
          color,
          type,
          price_range,
          count(*) as sales_count,
          sum(price) as sales_volume,
          avg(price) as sales_avg,
          min(price) as sales_min,
          max(price) as sales_max,
          approx_percentile(0.01) WITHIN GROUP (ORDER BY price) as sales_est_bottom_1_percent,
          approx_percentile(0.1) WITHIN GROUP (ORDER BY price) as sales_est_bottom_10_percent,
          approx_percentile(0.5) WITHIN GROUP (ORDER BY price) as sales_est_median,
          approx_percentile(0.90) WITHIN GROUP (ORDER BY price) as sales_est_top_10_percent,
          approx_percentile(0.99) WITHIN GROUP (ORDER BY price) as sales_est_top_1_percent
        FROM
          price_ranges
        GROUP BY name, region, color, type, price_range;
  "
  psql_remote -c "${bench_workload}"
}

export -f worst_benchmark

baseline_benchmark() {
  bench_workload="
    create materialized view m1
      with (backfill_order = NONE)
      as
        with price_ranges as (
          select
            car_info.name as name,
            car_regions.region as region,
            car_colors.color as color,
            car_types.type as type,
            car_sales.price as price,
            round(ln(1 + car_sales.price)::numeric, 1) as price_range
          from car_sales
            left join car_info on car_sales.car_id = car_info.id
            left join car_regions on car_sales.region_id = car_regions.id
            left join car_colors on car_sales.color_id = car_colors.id
            left join car_types on car_sales.type_id = car_types.id
        )
        select
          name,
          region,
          color,
          type,
          price_range,
          count(*) as sales_count,
          sum(price) as sales_volume,
          avg(price) as sales_avg,
          min(price) as sales_min,
          max(price) as sales_max,
          approx_percentile(0.01) WITHIN GROUP (ORDER BY price) as sales_est_bottom_1_percent,
          approx_percentile(0.1) WITHIN GROUP (ORDER BY price) as sales_est_bottom_10_percent,
          approx_percentile(0.5) WITHIN GROUP (ORDER BY price) as sales_est_median,
          approx_percentile(0.90) WITHIN GROUP (ORDER BY price) as sales_est_top_10_percent,
          approx_percentile(0.99) WITHIN GROUP (ORDER BY price) as sales_est_top_1_percent
        FROM
          price_ranges
        GROUP BY name, region, color, type, price_range;
  "
  psql_remote -c "${bench_workload}"
}

export -f baseline_benchmark

##################### Optimized

optimized_benchmark() {
  bench_workload="
    create materialized view m1
      with (backfill_order = FIXED(car_regions -> car_sales, car_info -> car_sales, car_colors -> car_sales, car_types -> car_sales))
      as
        with price_ranges as (
          select
            car_info.name as name,
            car_regions.region as region,
            car_colors.color as color,
            car_types.type as type,
            car_sales.price as price,
            round(ln(1 + car_sales.price)::numeric, 1) as price_range
          from car_sales
            left join car_info on car_sales.car_id = car_info.id
            left join car_regions on car_sales.region_id = car_regions.id
            left join car_colors on car_sales.color_id = car_colors.id
            left join car_types on car_sales.type_id = car_types.id
        )
        select
          name,
          region,
          color,
          type,
          price_range,
          count(*) as sales_count,
          sum(price) as sales_volume,
          avg(price) as sales_avg,
          min(price) as sales_min,
          max(price) as sales_max,
          approx_percentile(0.01) WITHIN GROUP (ORDER BY price) as sales_est_bottom_1_percent,
          approx_percentile(0.1) WITHIN GROUP (ORDER BY price) as sales_est_bottom_10_percent,
          approx_percentile(0.5) WITHIN GROUP (ORDER BY price) as sales_est_median,
          approx_percentile(0.90) WITHIN GROUP (ORDER BY price) as sales_est_top_10_percent,
          approx_percentile(0.99) WITHIN GROUP (ORDER BY price) as sales_est_top_1_percent
        FROM
          price_ranges
        GROUP BY name, region, color, type, price_range;
  "
  psql_remote -c "${bench_workload}"
}

export -f optimized_benchmark

hyperfine --shell=bash --runs 1 --show-output \
  --prepare 'conclude; prepare' --conclude 'conclude' baseline_benchmark \
  --prepare 'conclude; prepare' --conclude 'conclude' optimized_benchmark
