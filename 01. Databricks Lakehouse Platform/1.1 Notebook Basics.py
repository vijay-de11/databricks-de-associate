# Databricks notebook source
print("Day 1")

# COMMAND ----------

# MAGIC %md ### Magic Commands

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Title 1
# MAGIC ##  Title 2
# MAGIC #   Title 3
# MAGIC
# MAGIC text with **bold** and *italicized*
# MAGIC
# MAGIC Ordered list
# MAGIC 1. Apple
# MAGIC 1. Mango
# MAGIC 1. Orange
# MAGIC
# MAGIC Unordered list
# MAGIC * Apple
# MAGIC * Mango
# MAGIC * Orange
# MAGIC
# MAGIC Images
# MAGIC ![Databricks log](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>
# MAGIC

# COMMAND ----------

# MAGIC %md ### run magic command

# COMMAND ----------

# MAGIC %run ./includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %md ### fs magic command

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/')
print(files)

# COMMAND ----------

display(files)
