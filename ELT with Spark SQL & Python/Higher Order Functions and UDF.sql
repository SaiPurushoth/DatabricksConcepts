-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

select * from orders

-- COMMAND ----------

select
order_id,
books,
filter(books,i -> i.quantity >= 2 ) AS multiple_copies
from orders

-- COMMAND ----------

select * from(
select
order_id,
books,
filter(books,i -> i.quantity >= 2 ) AS multiple_copies
from orders
)
where size(multiple_copies) > 0

-- COMMAND ----------

select 
order_id,
books,
transform(books, b -> cast(b.subtotal * 0.8 as int)) AS subtotal_after_discount
from orders


-- COMMAND ----------

create or replace function get_url(email STRING)
returns STRING
return concat("https://www.",split(email,'@')[1])

-- COMMAND ----------

select email,get_url(email) As domain from customers

-- COMMAND ----------

describe function get_url

-- COMMAND ----------

describe function extended get_url;

-- COMMAND ----------

create function site_type(extension string)
returns string
return case 
when extension like '%.com' then 'commercial business'
when extension like '%.org' then 'non-profits organisation'
when extension like '%.edu' then 'educational institution'
else concat("Unknown Extension domain:",split(extension,'@')[1]) 
end;

-- COMMAND ----------

select email,site_type(email) as domain_type from customers

-- COMMAND ----------

-- drop function site_type;
-- drop function get_url;
