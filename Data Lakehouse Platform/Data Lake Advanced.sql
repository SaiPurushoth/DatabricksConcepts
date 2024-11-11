-- Databricks notebook source
-- MAGIC %md
-- MAGIC Advanced Data Lake features
-- MAGIC 1. restore table to timestamp or particular version
-- MAGIC 2. optimze the table by key column for effecient read and less size
-- MAGIC 3. vaccum to delete the old versioned files after the rettention period default 7 days

-- COMMAND ----------

describe history employees

-- COMMAND ----------

select * from employees version as of 1;

-- COMMAND ----------

delete from employees;

select * from employees;

-- COMMAND ----------

restore table employees version as of 1;

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

restore table employees version as of 2;

-- COMMAND ----------

select * from employees;

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

optimize employees
zorder by (id);

-- COMMAND ----------

describe detail employees;

-- COMMAND ----------

describe history employees;

-- COMMAND ----------

vacuum employees retain 1;

-- COMMAND ----------

drop table employees;

-- COMMAND ----------

restore table employees version as of 1;

-- COMMAND ----------


