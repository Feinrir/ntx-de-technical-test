--sql environment is using PostgreSQL
--creating blank table using column name and most likely data type from csv dataset
CREATE TABLE ecommerce_data (
    fullVisitorId TEXT,
    channelGrouping TEXT,
    time INTEGER,
    country TEXT,
    city TEXT,
    totalTransactionRevenue NUMERIC,
    transactions INTEGER,
    timeOnSite INTEGER,
    pageviews INTEGER,
    sessionQualityDim INTEGER,
    date DATE,
    visitId TEXT,
    type TEXT,
    productRefundAmount NUMERIC,
    productQuantity INTEGER,
    productPrice NUMERIC,
    productRevenue NUMERIC,
    productSKU TEXT,
    v2ProductName TEXT,
    v2ProductCategory TEXT,
    productVariant TEXT,
    currencyCode CHAR(3),
    itemQuantity INTEGER,
    itemRevenue NUMERIC,
    transactionRevenue NUMERIC,
    transactionId TEXT,
    pageTitle TEXT,
    searchKeyword TEXT,
    pagePathLevel1 TEXT,
    eCommerceAction_type INTEGER,
    eCommerceAction_step INTEGER,
    eCommerceAction_option TEXT
);

-- populating the blank table with the csv dataset from local source
copy ecommerce_data 
from 'D:\Creation\Coding\GitHub\ntx-de-technical-test\Soal 1 - Data Transformation dan Analysis Case\ecommerce-session-bigquery.csv'
DELIMITER ','
CSV HEADER;

-- doing a select * to familiarize myself with the data
-- i am fine with doing a select * since i know the data source is local and it is a relatively small amount of data
-- otherwise i would download small slice of the data and views it from an external program
select *
from ecommerce_data


/*
data familiarization based on data that is relevant to the task at hand
1. it is a session data taken from google bigquery, fullvisitorid and visitid can be concatenated together for a sessionid
2. total transaction revenue could be counted multiple times due to multiple product transaction in the same session results in the same total transaction revenue to appears in multiple rows
3. most of sessionqualitydim data value is null, while it is requested for the task, i don't think the resulting value is accurate for analysis purposes
4. this is a session table and not a transaction table which makes it bad for product analysis
5. there is multiple productsku that have multiple v2productname which makes grouping by v2productname inaccurate for product performance analysis
6. there is no productrevenue data, only totaltransactionrevenue which makes product performance analysis inaccurate
7. most of the productquantity value is null, will substitute with transaction amount but this is most likely inaccurate
8. there is no productrefundamount data, either there is no refund at all or there is some error that causing the data to not appear 
9. there is no transactionid data, either none of the transaction actually happens or there is some error
*/
