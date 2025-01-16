-- making sure that there is multiplecount of totaltransactionrevenue
with clean_revenue as (
select distinct fullvisitorid,visitid,country,totaltransactionrevenue,channelgrouping
from ecommerce_data
),

-- getting top 5 country with highest revenue
-- revenue rank is unnecesary if query performance is important but it increases useful details for the end results
top5 as (
select country,total_revenue,rank() over(order by total_revenue desc) as revenue_rank
from (
	select country,sum(totaltransactionrevenue) as total_revenue
	from ecommerce_data
	where totaltransactionrevenue is not null
	group by 1 
)
limit 5
)

-- query for end results
select t.revenue_rank,cr.country,cr.channelgrouping,sum(cr.totaltransactionrevenue) as total_revenue
from clean_revenue cr
join top5 t on cr.country = t.country
where cr.totaltransactionrevenue is not null
group by 1,2,3
order by 1,4 desc

/*
insight:
1. Referral revenue in the US is particularly high, more than double the next highest channel revenue
2. Venezuela, Canada, and Curacao relies mostly on organic search which means SEO is important for these countries
3. Taiwan only have referral channel grouping, either referral is the key driver in Taiwan or there could be error that causes other channel to not be tracked
4. Social media revenue is small in the US and absent from other countries
*/
