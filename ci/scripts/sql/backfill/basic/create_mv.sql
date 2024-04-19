SET STREAMING_PARALLELISM=1;
create materialized view mv1 as
with p1 as (
	select
		_id as id,
		(data ->> 'orders')::jsonb as orders
	from t1
),
p2 as (
	select
	 id,
	 orders ->> 'id' as order_id,
	 orders ->> 'price' as order_price,
	 orders ->> 'customer_id' as order_customer_id
	from p1
)
select
    id,
    order_id,
    order_price,
    order_customer_id
from p2;