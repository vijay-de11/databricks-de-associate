# Databricks notebook source
# MAGIC %md
# MAGIC - In this notebook, we will create a Delta Lake multi-hop pipeline.

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

# MAGIC %md
# MAGIC ### Creating a data stream

# COMMAND ----------

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC * Although the stream is created, but it will not be active until we do a display or write stream operation.
# MAGIC * Next, we will **enrich our raw data** with additional metadata describing the source file and the time it was ingested.
# MAGIC * Such information is useful for troubleshooting errors if corrupted data is encountered.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC    SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC    FROM orders_raw_temp
# MAGIC  )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp;

# COMMAND ----------

# MAGIC %md
# MAGIC * Now, we are going to pass this enriched data back to Pyspark API to process an incremental write to a Delta Lake table called **orders_bronze**.

# COMMAND ----------

(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_bronze;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_bronze;

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Static Lookup Table
# MAGIC * For the demo purpose, we need a static lookup table to join with our bronze table.

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup;

# COMMAND ----------

(spark.readStream
    .table("orders_bronze")
    .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Creating Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC    SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC           cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC    FROM orders_bronze_tmp o
# MAGIC    INNER JOIN customers_lookup c
# MAGIC    ON o.customer_id = c.customer_id
# MAGIC    WHERE quantity > 0)

# COMMAND ----------

(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us trigger another new file and wait for it to propagate through the previous two streams, from bronze to silver layer.

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Gold Table
# MAGIC * First, we need a stream of data from the silver table into a streaming temporary view
# MAGIC * Now, we will write another stream to create an aggregate gold table for the daily number of books for each customer.

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %md
# MAGIC * Note that, the stream stopped on its own after processing all the available data in micro batches.
# MAGIC * This is because we are using trigger **availableNow** option.
# MAGIC * With this way, we can combine streaming and batch workloads in the same pipeline.
# MAGIC * And we are also using the "complete" output mode to rewrite the updated aggregation each time our logic runs.
# MAGIC * However, keep in mind that the Structured streaming assumes data is only being appended in the upstream tables. Once a table is updated or overwritten, it is no longer valid for streaming. 
# MAGIC * So, in our case here, we cannot read a stream from this gold table.
# MAGIC * To change this behaviour we can set options like **ignoreChanges**, but they have other limitations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books;

# COMMAND ----------

# MAGIC %md
# MAGIC * Let us finally land all the remaining data files in our source directory.
# MAGIC * Now, the data will be propagated from our source directory into the bronze, silver until the gold layer.
# MAGIC * However, for the gold layer, we need to rerun our final query to update the gold table. Since, this query is configured as a batch job using the trigger **availableNow** syntax.

# COMMAND ----------

load_new_data(all=True)

# COMMAND ----------

# MAGIC %md ### Stop all the active streams by running the below for loop

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
