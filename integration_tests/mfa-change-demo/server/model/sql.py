GET_BATCH_FOR_TRAINING = """
select  l.fare_amount,c.*  from location_id_store l join converted_features c on l.window_start = c.window_start and l.do_location_id = c.do_location_id;
"""

GET_FEATURE = """
select * from converted_features where do_location_id=%s order by window_start desc limit 1;
"""