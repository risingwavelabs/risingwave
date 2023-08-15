GET_BATCH_FOR_TRAINING = """
select * from mv2;
"""

GET_FEATURE = """
select * from mv2 where do_location_id=%s order by window_start desc limit 1;
"""