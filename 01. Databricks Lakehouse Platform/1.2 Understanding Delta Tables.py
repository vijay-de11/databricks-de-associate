# Databricks notebook source
# MAGIC %md ### 01. Create an empty table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees
# MAGIC -- USING DELTA (since DELTA is the DEFAULT format so we don't need to specify it explicityly)
# MAGIC (id INT, name STRING, salary DOUBLE);

# COMMAND ----------

# MAGIC %md ### 02. Insert few records

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees
# MAGIC VALUES
# MAGIC (1, 'Vijay', 1000.00),
# MAGIC (2, 'Ajay', 2000.00),
# MAGIC (3, 'Rohit', 3000.00),
# MAGIC (4, 'Pooja', 4000.00),
# MAGIC (5, 'Bharti', 5000.00),
# MAGIC (6, 'Ruddu', 6000.00);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %md ### 03. To view table metadata info

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees;

# COMMAND ----------

# MAGIC %md ### 04. fs magic command

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md ### 05. Update command

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employees
# MAGIC SET salary = salary + 100
# MAGIC WHERE name LIKE 'A%';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees;

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

# COMMAND ----------

# MAGIC %md ###
# MAGIC * As we can see that 2 new files got added after update statement

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY employees;

# COMMAND ----------

# MAGIC %md ### Important Points
# MAGIC
# MAGIC * So, once we update the table data, there are 2 new files has been added to the directory. As, rather than updating the records in the files itself, we make a copy of them. And later, Delta uses the transaction log to indicate which files are valid in the current version of the table.
# MAGIC
# MAGIC * This can be confirmed after running the following command:
# MAGIC   **DESCRIBE DETAIL employees;** -- here we see that numFiles = 2
# MAGIC
# MAGIC * When we query our Delta table again, the query engine uses the transaction logs to resolve all the files that are valid in the current version and ignore all other data files.
# MAGIC
# MAGIC * And since the transaction logs also stores all the changes to the Delta lake table, we can easily review the table history using the **DESCRBIE HISTORY** command.
# MAGIC
# MAGIC * The transaction log is located under the _delta_log folder in the table directory.

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees/_delta_log/'

# COMMAND ----------

# MAGIC %fs head 'dbfs:/user/hive/warehouse/employees/_delta_log/00000000000000000002.json'
