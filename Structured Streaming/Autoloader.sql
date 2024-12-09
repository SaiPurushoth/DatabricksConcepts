-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint").load(f'{dataset_bookstore}/orders-raw').writeStream.option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint").table('order_updates')
-- MAGIC )

-- COMMAND ----------

select * from order_updates;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f'{dataset_bookstore}/orders-raw')
-- MAGIC display(files)

-- COMMAND ----------

select * from order_updates;

-- COMMAND ----------

describe history order_updates

-- COMMAND ----------

drop table order_updates

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm('dbfs:/mnt/demo/orders_checkpoint',True)
