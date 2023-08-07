create table actionhistory(
  userid varchar,
  eventype int, -- 1 is mfa+ , 0 is mfa- , other is other
  timestamp timestamp,
  changenum int,
);

insert into actionhistory values
    ('user1', 1, '2016-02-01 00:00:01',50),
    ('user1', 0, '2016-02-01 00:00:03',50),
    ('user1', 1, '2016-02-01 00:00:05',100),
    ('user1', 2, '2016-02-01 00:01:07',50),
    ('user1', 2, '2016-02-01 00:01:09',20),
    ('user1', 0, '2016-02-01 00:01:11',50),
    ('user2', 0, '2016-02-01 00:00:13',50),
    ('user2', 2, '2016-02-01 00:00:15',10),
    ('user2', 2, '2016-02-01 00:00:17',10),
    ('user2', 1, '2016-02-01 00:01:19',50),
    ('user2', 2, '2016-02-01 00:01:21',10),
    ('user2', 1, '2016-02-01 00:01:23',20);


create materialized view user_action_mfa as select userid, timestamp,changenum,eventype from actionhistory where eventype in (1,2);

create materialized view user_mfa_change_count as 
      select userid , count(*) as count, window_start
      from(
        select * from tumble(user_action_mfa , timestamp , INTERVAL '30 minutes')
      ) group by userid,window_start;


create function udf_sum(int,int) returns int as udf_sum using link 'http://localhost:8815';

create materialized view user_mfa_change_num as 
      select userid , sum(udf_sum(changenum,eventype)) as sum, window_start
      from(
        select * from tumble(user_action_mfa , timestamp , INTERVAL '30 minutes')
      ) group by userid,window_start;


