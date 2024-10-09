# Databricks notebook source
# MAGIC %md ### 
# MAGIC
# MAGIC In this, we will learn about the following:
# MAGIC * Time Travel
# MAGIC * Optimize
# MAGIC * ZORDER BY
# MAGIC * VACCUM

# COMMAND ----------

# MAGIC %md ### Time Travel

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees@v1;

# COMMAND ----------

# MAGIC %md
# MAGIC Let say if accidently we delete the data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md
# MAGIC * We can restore the data by using **RESTORE** command

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE employees TO VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * Since sparks works in parallel, we usually end up by writing too many small files.
# MAGIC * Having many small data files negatively affect the performance of the Delta table.
# MAGIC * Here we have numFiles = 2, but it can be a large no also.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %md
# MAGIC * We can use **OPTIMIZE** command that combine files toward an optimal size.
# MAGIC * **OPTIMIZE** will replace existing data files by combining records and rewriting the results.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE employees
# MAGIC ZORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %md
# MAGIC **ZORDER indexing speeds up data retrieval when filtering on provided fields, by grouping data with similar values within the same data files**
# MAGIC
# MAGIC * However, on smaller dataset we can't see much performance gain

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md
# MAGIC * We know that our current table version referencing only one file(after the OPTIMIZE operation)
# MAGIC * It means that the other data files are unused files and we can clean them up.
# MAGIC * We can manually remove old data files using the **VACUUM** command.

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md 
# MAGIC * It seems even after executing VACUUM command nothing has happened and the data files still persists.
# MAGIC * This is because we need to specify the **retention period** , and by default this retention period is **7 days**
# MAGIC * This means that VACUUM operation will prevent us from deleting files less than 7 days old, if we try to explicitly give some no less than 7 then it won't work because by default is 7 days.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC * In this demo, we will do a workaround for demonstration purpose only.
# MAGIC * **This shouldn't be done in production**.
# MAGIC * We will turn off the retention duration check using the below command.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM employees RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md
# MAGIC * Now the data files are deleted successfully.
# MAGIC * Now, we will no longer be able to do the time travel.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees VERSION AS OF 1;

# COMMAND ----------

# MAGIC %md
# MAGIC * Ideally, we should get a file not found exception, because the data files for this version are no longer exist.
# MAGIC * Finally, let us permanently delete the table with its data from the Lakehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'
