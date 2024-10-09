-- Databricks notebook source
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Try to use the **TEMPORARY VIEW** created in Session 1 of the Views

-- COMMAND ----------

SELECT * FROM temp_view_phones_brand;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * So temporary views are not accessible, for example, from another notebook as we did above.
-- MAGIC * Or after detaching and reattaching a notebook to a cluster or after installing a python package which in turn starts the Python interpretor, or simply after restarting the cluster.

-- COMMAND ----------

-- MAGIC %md ### What about global temp views?

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * As long as the cluster is running, the database **global_temp** persist, and any notebook attached to the cluster can access its **global temporary views**
-- MAGIC * But if the cluster is terminated and if we restart the **global temp view** will no longer exists.

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop table and views

-- COMMAND ----------

DROP TABLE smartphones;
DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;
