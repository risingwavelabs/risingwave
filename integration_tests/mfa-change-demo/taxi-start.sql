create source if not exists taxiallfeature (
    vendor_id int,
    lpep_pickup_datetime timestamp,
    lpep_dropoff_datetime timestamp,
    store_and_fwd_flag boolean,
    ratecode_id float,
    pu_location_id int,
    do_location_id int,
    passenger_count float,
    trip_distance float,
    fare_amount float,
    extra float,
    mta_tax float,
    tip_amount float,
    tolls_amount float,
    ehail_fee float,
    improvement_surcharge float,
    total_amount float,
    payment_type float,
    trip_type float,
    congestion_surcharge float,
) with (
    connector = 'kafka',
    topic = 'taxi',
    properties.bootstrap.server = 'kafka:9092',
)
FORMAT PLAIN ENCODE JSON;

create materialized view mv1 as select lpep_pickup_datetime ,lpep_dropoff_datetime ,do_location_id ,
    passenger_count ,trip_distance ,fare_amount ,extra ,mta_tax ,
    tip_amount,tolls_amount ,improvement_surcharge ,total_amount,
    congestion_surcharge from taxiallfeature where payment_type in (1,2,4);

create materialized view mv2 as select
    do_location_id,
    avg(fare_amount) as avg_amount,
    window_start,
    avg(EXTRACT(EPOCH FROM lpep_dropoff_datetime - lpep_pickup_datetime)::INT) / 10 as latency,
    avg(passenger_count) as passenger_count,
    avg(trip_distance) as trip_distance,
    avg(extra) as extra,
    avg(mta_tax) as mta_tax,
    avg(tip_amount) as tip_amount,
    avg(tolls_amount) as tolls_amount,
    avg(improvement_surcharge) as improvement_surcharge,
    avg(total_amount) as total_amount,
    avg(congestion_surcharge) as congestion_surcharge,
    avg(trip_distance) > 30 as long_distance
from (
    select * from tumble(mv1,lpep_pickup_datetime,INTERVAL '5' hour)
) group by do_location_id,window_start;