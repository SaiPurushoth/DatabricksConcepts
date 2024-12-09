-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![](../Includes/bookstore_schema.png)

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint").load(f'{dataset_bookstore}/orders-raw').createOrReplaceTempView('order_raw_temp')
-- MAGIC )

-- COMMAND ----------

create or replace temporary view orders_temp as (
  select * , current_timestamp() arrival_time, input_file_name() input_file_name
  from order_raw_temp
)

-- COMMAND ----------

select * from orders_temp

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table('orders_temp').writeStream.format('delta').option("checkpointLocation", "dbfs:/mnt/demo/orders_bronze").outputMode('append').table('order_bronze')
-- MAGIC )

-- COMMAND ----------

select * from order_bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.read.format('json').load(f'{dataset_bookstore}/customers-json').createOrReplaceTempView('customers_lookup')
-- MAGIC )

-- COMMAND ----------

select * from customers_lookup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC   .table("order_bronze")
-- MAGIC   .createOrReplaceTempView("orders_bronze_tmp")
-- MAGIC   )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp)  order_timestamp, books
FROM orders_bronze_tmp o
INNER JOIN customers_lookup c
ON o.customer_id = c.customer_id
WHERE quantity > 0)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC   spark.table("orders_enriched_tmp")
-- MAGIC       .writeStream
-- MAGIC       .format("delta")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
-- MAGIC       .outputMode("append")
-- MAGIC       .table("orders_silver"))

-- COMMAND ----------

select * from orders_silver

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.readStream
-- MAGIC   .table("orders_silver")
-- MAGIC   .createOrReplaceTempView("orders_silver_tmp"))

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
FROM orders_silver_tmp
GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
 )

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.table("daily_customer_books_tmp")
-- MAGIC       .writeStream
-- MAGIC       .format("delta")
-- MAGIC       .outputMode("complete")
-- MAGIC       .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
-- MAGIC       .trigger(availableNow=True)
-- MAGIC       .table("daily_customer_books"))

-- COMMAND ----------

select * from daily_customer_books

-- COMMAND ----------

-- MAGIC %python
-- MAGIC load_new_data(all=True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](../Includes/)
