# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

load_new_data()

# COMMAND ----------

# load_new_json_data()

# COMMAND ----------

# %sql
# select * from json.`${dataset.bookstore}/books-cdc/02.json`

# COMMAND ----------

# MAGIC %md
# MAGIC - **row_time** will be used as a sequence key in our CDC data processing.
# MAGIC - Switch to DLT pipeline and create a new pipeline from there Books pipeline
