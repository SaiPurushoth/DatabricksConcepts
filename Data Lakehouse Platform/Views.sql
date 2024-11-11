-- Databricks notebook source
create table smartphones(id int,name string,brand string,year int)


-- COMMAND ----------

insert into smartphones values(1,'iphone 14','apple',2022),(2,'pixel 6','google',2022),(3,'oneplus 8','oneplus',2023),(4,'vivo y95','vivo',2023),(5,'oppo a53','oppo',2021),(6,'xiaomi mi 11','xiaomi',2022),(7,'huawei p40','huawei',2021),(8,'asus zenfone 6','asus',2019),(9,'oneplus 7T','oneplus',2018),(10,'galaxy s21',' Samsung',2014),(11,'galaxy s21+',' Samsung',2022)

-- COMMAND ----------

create view smartphones_view as select * from smartphones where year=2022 

-- COMMAND ----------

select * from smartphones_view;

-- COMMAND ----------

create temp view smartphones_temp_view as select * from smartphones where year=2023;

-- COMMAND ----------

select * from smartphones_temp_view

-- COMMAND ----------

show tables;

-- COMMAND ----------

create global temporary view smartphones_temp_view as select * from smartphones where brand IN ('oneplus', 'xiaomi');

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.smartphones_temp_view;

-- COMMAND ----------


