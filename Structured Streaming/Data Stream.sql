-- Databricks notebook source
-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.readStream.table('Books').createOrReplaceTempView('books_streaming_tmp_vw')
-- MAGIC )

-- COMMAND ----------

select * from 
books_streaming_tmp_vw;

-- COMMAND ----------

select author, count(book_id) AS total_books
from books_streaming_tmp_vw
group by author

-- COMMAND ----------

select * from books_streaming_tmp_vw order by author;

-- COMMAND ----------

create or replace temp view author_count_tmp_vw as (
select author, count(book_id) AS total_books
from books_streaming_tmp_vw
group by author
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table('author_count_tmp_vw').writeStream.trigger(processingTime='4 seconds').outputMode('complete').option('checkpointLocation', '/tmp/checkpoint/author_count_tmp_vw').table('author_counts')
-- MAGIC
-- MAGIC )

-- COMMAND ----------

select * from author_counts;

-- COMMAND ----------

insert into books values ('B19',"Introduction to modeling and simulation","Jane Smith","computer science", 25), ('B20',"Robot Modeling and Control","Jane Smith","computer science", 30), ('B21','Turning: Vision: The Birth of Computer Science','Chris Brown','Computer Science',35);

-- COMMAND ----------

INSERT INTO books
 values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
 ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC spark.table('author_count_tmp_vw').writeStream.trigger(availableNow=True).outputMode('complete').option('checkpointLocation', '/tmp/checkpoint/author_count_tmp_vw').table('author_counts').awaitTermination()
-- MAGIC )

-- COMMAND ----------

select * from author_counts

-- COMMAND ----------

INSERT INTO books
 values ("B22", "Hands-On Deep Learning Algorithms with Python", "Sachinar Ravichandiran", "Computer Science", 25),
 ("B23", "Neural Network Methods in Natural Language Processing", "hamad ali", "Computer Science", 30),
("B24", "Understanding digital signal processing", "nichol sparks", "Computer Science", 35)
