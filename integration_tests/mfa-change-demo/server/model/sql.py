GET_BATCH_FOR_TRAINING = """
select * from converted_features;
"""

GET_FEATURE = """
select * from converted_features where do_location_id=%s order by window_start desc limit 1;
"""