-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

select * from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select *,input_file_name() AS source_file_name from json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

select * from csv.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

create table books_csv 
(book_id string,title string,author string,category string,price double)
using csv
options (
  header = true,
  delimiter = ';'
)
location '${dataset.bookstore}/books-csv'

-- COMMAND ----------

select * from books_csv

-- COMMAND ----------

describe extended books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read.table('books_csv').write.mode('append').format('csv').option('header', 'true').save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

refresh table books_csv

-- COMMAND ----------

select count(*) from books_csv

-- COMMAND ----------

create table customers as select * from json.`${dataset.bookstore}/customers-json`;

describe extended customers

-- COMMAND ----------

create table books_unparsed as select * from csv.`${dataset.bookstore}/books-csv`;

select * from books_unparsed;

-- COMMAND ----------

create temp view books_tmp_vw 
(book_id string, title string, author string,category string, price double) 
using csv 
options (path "${dataset.bookstore}/books-csv/export_*.csv", header "true",delimiter ";")
;

create table books as select * from books_tmp_vw;

select * from books;


-- COMMAND ----------

describe extended books;
