create source if not exists actionhistory (
    userid varchar,
    eventype int, -- '1' is mfa , other is other
    timestamp timestamp,
    changenum int,
) with (
    connector = 'kafka',
    topic = 'recwave',
    properties.bootstrap.server = 'localhost:9092',
)
FORMAT PLAIN ENCODE JSON;

create materialized view user_action_mfa as select * from actionhistory where eventype in (1,2);
create materialized view user_mfa_change_count as 
      select userid , count(*) as count, window_start
      from(
        select * from tumble(user_action_mfa , timestamp , INTERVAL '30 minutes')
      ) group by userid,window_start;


create function udf_sum(int,int) returns int as udf_sum using link 'http://localhost:8815';

create materialized view user_mfa_change_sum as 
      select userid , sum(udf_sum(changenum,eventype)) as udf_sum, window_start
      from(
        select * from tumble(user_action_mfa , timestamp , INTERVAL '30 minutes')
      ) group by userid,window_start;