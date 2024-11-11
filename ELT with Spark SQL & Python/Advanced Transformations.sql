-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

select * from customers

-- COMMAND ----------

describe history customers

-- COMMAND ----------

select customer_id, profile:first_name, profile:address:country from customers

-- COMMAND ----------

select from_json(profile) from customers

-- COMMAND ----------

select profile from customers limit 1

-- COMMAND ----------

create or replace temp view parsed_profile as select customer_id,from_json(profile,schema_of_json('
{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) as profile from customers;

select * from parsed_profile;

-- COMMAND ----------

select customer_id,profile.first_name,profile.last_name from parsed_profile;

-- COMMAND ----------

create or replace temp view customer_final as select customer_id,profile.* from parsed_profile;

select * from customer_final

-- COMMAND ----------

select order_id,customer_id,books from orders;

-- COMMAND ----------

select order_id,customer_id,explode(books) from orders;

-- COMMAND ----------

select customer_id,
collect_set(order_id),
collect_set(books.book_id)
from orders
group by customer_id;

-- COMMAND ----------

select customer_id,
collect_set(books.book_id) AS before_flattern,
array_distinct(flatten(collect_set(books.book_id))) AS after_flattern
from orders
group by customer_id;

-- COMMAND ----------

create or replace temp view orders_enriched AS select * 
from books
inner join 
(select customer_id,explode(books) AS book from orders) order 
on order.book.book_id = books.book_id;

select * from orders_enriched;

-- COMMAND ----------

create or replace temp view order_updates AS select * from parquet.`${dataset.bookstore}/orders-new`;

select * from orders
union
select * from order_updates;


-- COMMAND ----------

select * from orders
intersect
select * from order_updates;

-- COMMAND ----------

select * from orders
minus
select * from order_updates;

-- COMMAND ----------

create or replace table transactions AS
select * from(
select 
customer_id,
book.book_id,
book.quantity 
from orders_enriched)
pivot(sum(quantity) for book_id in ('B01','B02','B03','B04','B05','B06','B07','B08','B09','B10','B11','B12'));

select * from transactions;

-- COMMAND ----------


