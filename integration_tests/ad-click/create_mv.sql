-- The number of clicks on the ad within one minute after the ad was shown.
create materialized view m_click_statistic as
select
    count(user_id) as clicks_count,
    ad_id
from
    ad_source
where
    click_timestamp is not null
    and impression_timestamp < click_timestamp
    and impression_timestamp + interval '1' minute >= click_timestamp
group by
    ad_id;