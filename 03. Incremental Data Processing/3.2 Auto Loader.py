# Databricks notebook source
# MAGIC %md
# MAGIC * In this notebook, we will explore incremental data ingestion from files using Auto Loader.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md ### Auto Loader

# COMMAND ----------

(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %md
# MAGIC * Since, Auto Loader use spark structured streaming, so the above query will continue to run unless we stop it explicity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us add some data to the source directory using load_new_data() utility function, which will add 1000 records on each call.

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC * So, we have successfully added 2 data files to our source directory.
# MAGIC * When we look at the streaming dashboard which is still active, we see that Auto Loader has detected automatically that there are new files in our source directory and it started processing them.
# MAGIC * By now, our table would have been updated with new records from Auto Loader.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_updates;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates;

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us drop the drop tha table and remove the checkpoint location.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates;

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
