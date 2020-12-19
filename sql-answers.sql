-- 1) to check which records are ONLY in SPL_STG.
select * from SPL_STG 
left join SPL on SPL_STG.source_file = SPL.source_file
where SPL.source_file IS NULL;

-- 2)
select Publishers.publisher_account_name
from SPL
join Publishers
on Publishers.gid = SPL.effectiveid
where LOWER(useragent) LIKE '%mozilla/%' OR LOWER(useragent) LIKE '%ff/%'
group by Publishers.publisher_account_name
having count(*) > 50;

-- 3)
select
    DATE_FORMAT(dt_datetime, "%Y%m%d") AS dt,
    DATE_FORMAT(dt_datetime, "%H") AS dt_hour_id,
    sum(IF searchactions IN ('Search', 'AdsSearch') THEN 1 ELSE 0 END) AS searches,
    sum(IF searchactions IN ('AdsResultClick', 'SearchResultClick') THEN 1 ELSE 0 END) AS clicks,
    sum(IF searchactions IN ('SiteLink', 'Sponsored') THEN 1 ELSE 0 END) AS Sponsored_clicks,
    sum(IF searchactions IS NULL THEN 1 ELSE 0 END) AS Organic_clicks
from SPL
group by DATE_FORMAT(dt_datetime, "%Y%m%d"), DATE_FORMAT(dt_datetime, "%H")

-- 4)
select d.dt, d.hour_id, d.searches, d.acc_searches
from (
    select
        dt,
        hour_id,
        searches,
        SUM(searches) OVER (ORDER BY dt) acc_searches,
        RANK() OVER(PARTITION BY dt ORDER BY searches DESC) r
    from spl
) d
where d.r = 3

-- 5) assuming the data in the tables is already distinct - using UNION instead of UINION ALL.
-- internal
select 
    dt,
    hour_id,
    device,
    country_code,
    SUM(searches) AS int_searches,
    SUM(clicks) AS int_clicks,
    NULL AS ext_searches,
    NULL AS ext_clicks,
    NULL AS payout,
    'Bing' provider
from internal_data
where searchprovider = 2

UNION

-- external
select
    dt,
    hour_id,
    device,
    country_code,
    NULL AS int_searches,
    NULL AS int_clicks,
    SUM(searches) AS ext_searches,
    SUM(clicks) AS ext_clicks,
    NULL AS payout,
    'Bing' provider
from external_data
where file_description = "Bing Feed AdUnit"

UNION

-- payout
select 
    dt,
    hour_id,
    device,
    country_code,
    NULL AS int_searches,
    NULL AS int_clicks,
    NULL AS ext_searches,
    NULL AS ext_clicks,
    SUM(payout) AS payout,
    'Bing' provider
from payout
where search_provider_id = 2

-- 6)

select
  x.dt, x.search_time,
  DENSE_RANK() OVER(PARTITION BY x.dt ORDER BY case when x.date_diff>5 then 1 end) session_group
from(
  select
    *,
    LAG(search_time) OVER(ORDER BY search_time) as previous_search_time,
    ABS(datediff(minute, LAG(search_time) OVER(ORDER BY search_time), search_time)) date_diff
  from search_session
) x








