# Databricks notebook source
files = dbutils.fs.ls('dbfs:/mnt/demo/dlt/demo_bookstore')
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC - The **system** directory captures all the events associated with the pipeline.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls('dbfs:/mnt/demo/dlt/demo_bookstore/system/events')
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`
# MAGIC
# MAGIC -- All the events we see in the UI are stored in this delta table

# COMMAND ----------

files = dbutils.fs.ls('dbfs:/mnt/demo/dlt/demo_bookstore/tables/')
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC - Go to DLT pipeline and copy the table information by clicking on any table [Table Name: demo_bookstore_dlt_db.cn_daily_customer_books]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_bookstore_dlt_db.cn_daily_customer_books;
