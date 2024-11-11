-- Databricks notebook source
-- MAGIC %md
-- MAGIC Basics of Building Data Lake:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create sample table as delta table (Hive)

-- COMMAND ----------

create table employees(id int,name varchar(20),salary double)

-- COMMAND ----------

insert into employees values (1,"john",3500),(2,"alice",3000),(3,"bob",4000),(4,"carol",2000),(5,"dave",5000),(6,"eve",4500)

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Store the data as partionings in parquet format

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files=dbutils.fs.ls('dbfs:/user/hive/warehouse/employees')
-- MAGIC display(files)

-- COMMAND ----------

update employees set name = 'John' where id = 1;
update employees set name = 'Jane' where id = 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Update will create a new parquet file with updated value and create a new delta log taransaction to read from latest file and keep the old parquet file for versioning.

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe history employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files=dbutils.fs.ls('dbfs:/user/hive/warehouse/employees/_delta_log')
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file=dbutils.fs.head('dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000004.json')
-- MAGIC display(file)

-- COMMAND ----------


