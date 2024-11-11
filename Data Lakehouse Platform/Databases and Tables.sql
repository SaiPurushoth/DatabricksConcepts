-- Databricks notebook source
-- MAGIC %md
-- MAGIC We can create external schema and tables from location specified , apart from default database.
-- MAGIC
-- MAGIC to note here:
-- MAGIC 1. if you have specified the location of file store path like s3 or blob , then if you have dropped the tables the these table files won't get deleted since it external type
-- MAGIC 2. but in case of not specifiying and creating tables by only create table commands and those files will also got deleted since it in type managed
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC shallow copy and deep copy
-- MAGIC
-- MAGIC create table new_table
-- MAGIC [shallow clone/deep clone] old_table
-- MAGIC
-- MAGIC
-- MAGIC will refer schema and data type from old table and create new table and insert data to it.
-- MAGIC deep clone takes time insert data as well.
-- MAGIC shallow clone copy only transaction log files

-- COMMAND ----------

create table external_default (width int,length int,height int)
location 'dbfs:/FileStore/tables/external_default'


-- COMMAND ----------

insert into external_default values (1,3,5)

-- COMMAND ----------

describe extended external_default;

-- COMMAND ----------

create schema if not exists external_schema;

-- COMMAND ----------

use external_schema;

-- COMMAND ----------

create table external_table (id int, name string);

-- COMMAND ----------

describe extended external_table;

-- COMMAND ----------

show tables;
