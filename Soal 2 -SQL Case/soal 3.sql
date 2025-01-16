-- making sure that there is multiplecount of totaltransactionrevenue
-- also getting total of product price per visitid
with clean_revenue as (
select distinct fullvisitorid,visitid,totaltransactionrevenue,sum(productprice) as total_product_price
from ecommerce_data
where totaltransactionrevenue is not null
group by 1,2,3
),

-- because we don't have product revenue, i am trying to approximate the product revenue by using the product price proportion from the whole visit which i assume there is one transaction per visit
-- task requires using v2productname but i still think it is better to use productsku instead
product_revenue as (
select distinct ed.fullvisitorid, ed.visitid, ed.v2productname,
case
	when total_product_price = 0 then cr.totaltransactionrevenue
else (ed.productprice/cr.total_product_price) * cr.totaltransactionrevenue end as product_revenue,
ed.transactions,
ed.productquantity,
ed.productrefundamount
from ecommerce_data ed
left join clean_revenue cr on ed.fullvisitorid = cr.fullvisitorid and ed.visitid = cr.visitid
)

-- query for end results, if product quantity data is available the it is used for total product sold, otherwise transaction data is used
-- since there is no refund data, it is impossible to get net revenue or flag product with refund higher than 10% of revenue
select v2productname,
sum(product_revenue) as total_revenue,
sum(coalesce(productquantity,transactions)) as total_sold,
sum(productrefundamount) as totalrefund
from product_revenue
group by 1
order by 2 desc nulls last

/*
insight:
as stated before, the data is product performance is mostly unavailable that i don't think it's possible to get meaningful insight out of the data
we need data from another dataset or fix the current dataset before trying to analyse any further
*/
