GET_BATCH_FOR_TRAINING = """
select  l.fare_amount,d.*, p.*
from location_id_store l
join converted_features_with_do_location d on l.window_start = d.window_start and l.do_location_id = d.do_location_id
join converted_features_with_pu_location p on l.window_start = p.window_start and l.pu_location_id = p.pu_location_id;
"""

GET_FEATURE_DO_LOCATION = """
select * from converted_features_with_do_location where do_location_id=%s order by window_start desc limit 1;
"""

GET_FEATURE_PU_LOCATION = """
select * from converted_features_with_pu_location where pu_location_id=%s order by window_start desc limit 1;
"""