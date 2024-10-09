# Databricks notebook source
# MAGIC %md ### Create and insert data into a managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE managed_default
# MAGIC (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_default
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_default;

# COMMAND ----------

# MAGIC %md ### Create an external location

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE external_default
# MAGIC (width INT, length INT, height INT)
# MAGIC LOCATION 'dbfs:/mnt/demo/external_default';
# MAGIC
# MAGIC INSERT INTO external_default
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_default;

# COMMAND ----------

# MAGIC %md ### Drop managed table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/managed_default';

# COMMAND ----------

# MAGIC %md ### Drop an external table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_default'

# COMMAND ----------

# MAGIC %md ### Create a new database 
# MAGIC * We can use either SCHEMA or DATABASE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA new_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED new_default;

# COMMAND ----------

# MAGIC %md ### Create table and insert records

# COMMAND ----------

# MAGIC %sql
# MAGIC USE new_default;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create managed table
# MAGIC CREATE TABLE managed_new_default
# MAGIC (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_new_default
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create external table
# MAGIC CREATE TABLE external_new_default 
# MAGIC (width INT, length INT, height INT) 
# MAGIC LOCATION 'dbfs:/mnt/demo/external_new_default';
# MAGIC
# MAGIC INSERT INTO external_new_default
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_new_default

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_new_default;

# COMMAND ----------

# MAGIC %md ### Drop managed and external tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_new_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/new_default'

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE external_new_default;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_new_default'

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Create a database in a custom location outside of the hive directory

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA custom
# MAGIC LOCATION 'dbfs:/Shared/schemas/custom.db'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED custom;

# COMMAND ----------

# MAGIC %md 
# MAGIC * Although, it appears that the custom database is created in the hive metastore, but if we execute **DESCRIBE DATABASE EXTENDED custom** command, we see that the Location is custom location which we have given while creation of the database which is outside of the hive directory i.e. 'dbfs:/Shared/schemas/custom.db'

# COMMAND ----------

# MAGIC %md ### Create managed and external table in custom database

# COMMAND ----------

# MAGIC %sql
# MAGIC USE custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create managed table
# MAGIC CREATE TABLE managed_custom
# MAGIC (width INT, length INT, height INT);
# MAGIC
# MAGIC INSERT INTO managed_custom
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED managed_custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create external table
# MAGIC CREATE TABLE external_custom
# MAGIC (width INT, length INT, height INT) 
# MAGIC LOCATION 'dbfs:/mnt/demo/external_custom';
# MAGIC
# MAGIC INSERT INTO external_custom
# MAGIC VALUES
# MAGIC (3, 2, 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED external_custom;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE managed_custom;
# MAGIC DROP TABLE external_custom;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/Shared/schemas/custom'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/mnt/demo/external_custom'
