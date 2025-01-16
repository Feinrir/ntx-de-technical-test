-- average timeonsite, pageviews, and sessionqualitydim for each visitorid
with average_user as (
select fullvisitorid,
avg(timeonsite) as average_time,
avg(pageviews) as average_pageviews,
avg(sessionqualitydim) as average_session_quality
from ecommerce_data
where timeonsite is not null and pageviews is not null
group by 1
),

-- average timeonsite, pageviews, and sessionqualitydim overall for comparison
average_overall as (
select avg(timeonsite) as average_time,
avg(pageviews) as average_pageviews,
avg(sessionqualitydim) as average_session_quality
from ecommerce_data
where timeonsite is not null and pageviews is not null
)

-- getting visitorid with higher average time but lesser pageview
select fullvisitorid,
au.average_time,
au.average_pageviews,
au.average_session_quality,
ao.average_time as overall_average_time,
ao.average_pageviews as overall_average_pageviews,
ao.average_session_quality as overall_average_session_quality
from average_user au
cross join average_overall ao 
where au.average_time > ao.average_time and au.average_pageviews < ao.average_pageviews

/*
insight:
there is not enough sessionqualitydim value for it to be useful in an analysis, as it is this data can't be used and need to be further analysed by seeing what pages are the users with these visitorid visiting
*/