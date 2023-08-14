"""
todo:
the batch query on source isn't permitted
however, the recall can be based on a roughly calculated score
the score for each action is counted
if so, the get_most_interacted is going to need a better model

"""

GET_BATCH_FOR_TRAINING = """
select * from mv2;
"""

GET_FEATURE = """
select * from mv2 where do_location_id=%s order by window_start desc limit 1;
"""