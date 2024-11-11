-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

create table orders as select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select * from orders

-- COMMAND ----------

create or replace table orders as select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

describe history orders

-- COMMAND ----------

insert overwrite orders select *,current_timezone() from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

insert into orders select * from parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

select count(*) from orders

-- COMMAND ----------

create or replace temp view customer_updates as select * from json.`${dataset.bookstore}/customers-json-new`;

merge into customers c
using customer_updates u
on c.customer_id = u.customer_id
when matched and c.email is null and u.email is not null then update set c.email = u.email, c.updated = u.updated
when not matched then insert *

-- COMMAND ----------

create or replace temp view book_updates
using csv
options (path = '${dataset.bookstore}/books-csv-new',
header = "true",
delimiter = ';');

select * from book_updates

-- COMMAND ----------

merge into books b
using book_updates u
on b.book_id = u.book_id
when not matched AND u.category = 'Computer Science' then insert *

-- COMMAND ----------

describe history books 

-- COMMAND ----------

describe history customers
