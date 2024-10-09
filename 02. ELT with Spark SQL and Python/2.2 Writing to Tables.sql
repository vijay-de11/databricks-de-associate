-- Databricks notebook source
-- MAGIC %md
-- MAGIC * In this notebook, we are going to explore SQL syntax to insert and update records in Delta Tables.
-- MAGIC * Delta technology provides ACID compliant updates to Delta tables.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Since parquet files have a well-defined schema, so we managed to extract the data correctly.
-- MAGIC * When writing to tables, we could be interested by completely overwriting the data in the table.
-- MAGIC * In fact, there are multiple benefits of overwriting tables instead of deleting and recreating tables.
-- MAGIC * For example, the old version of the table still exists and can easily retrieve all data using Time Travel.
-- MAGIC * In addition, overwriting a table is much faster because it does not need to list the directory recursively or delete any files.
-- MAGIC * In addition, its an atomic operation.
-- MAGIC * Concurrent queries can still read the table while you are overwriting it.
-- MAGIC * Due to the ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###  The various methods to accomplish complete overwrite is to use :
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. CREATE OR REPLACE TABLE (also known as CRAS statement)
-- MAGIC * CREATE OR REPLACE TABLE statements fully replace the content of a table each time they execute.``

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. INSERT OVERWRITE statement
-- MAGIC * It provides a nearly identical output as above.
-- MAGIC * It means data in the target table will be replaced by data from the query.
-- MAGIC * However, INSERT OVERWRITE has some differences:
-- MAGIC   * It can only overwrite an existing table and not creating a new one like our CREATE OR REPLACE statement.
-- MAGIC   * It can overwrite only the new records that match the current table schema which means that it is a safer technique for overwriting an existing table without the risk of modifying the table schema.

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

DESCRIBE HISTORY orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * The INSERT OVERWRITE operation has been recorded as a new version in the table as WRITE operation , and it has the mode OVERWRITE.
-- MAGIC * If we try to INSERT OVERWRITE the data with different schema, for example here we are adding a new column of the data for the current_timestamp(), by running the below command we will see an exception.
-- MAGIC * So the way how DELTA LAKE enforce schema on-write is the primary difference between INSERT OVERWRITE and CREATE OR REPLACE TABLE statements.

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending records to the table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. INSERT INTO 
-- MAGIC * The INSERT INTO statement is a simple and efficient operation for inserting new data.
-- MAGIC * However, it doesn't have any built in guarantees to prevent inserting the same records multiple times.
-- MAGIC * It means, reexecuting the query will write the same records to the target table resulting in duplicate records.
-- MAGIC * To resolve this issue, we can use our second method, which is **MERGE INTO statement**

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT COUNT(*) FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. MERGE INTO statement
-- MAGIC * With the merge statement, you can upsert data from a source table, view, or dataframe into the target delta table.
-- MAGIC * It means you can insert, update and delete using the MERGE INTO statement.
-- MAGIC * So, in a merge operaion, updates, inserts and deletes are completed in a single atomic transaction.
-- MAGIC * In addition, merge operation is a great solution for avoiding duplicates when inserting records.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates cu
ON c.customer_id = cu.customer_id
WHEN MATCHED AND c.email IS NULL AND cu.email IS NOT NULL THEN
UPDATE SET c.email = cu.email, updated = cu.updated
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates 
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS(
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates;

-- COMMAND ----------

MERGE INTO books b
USING books_updates bu
ON b.book_id = bu.book_id AND b.title = bu.title
WHEN NOT MATCHED AND bu.category = 'Computer Science' THEN INSERT *;

-- COMMAND ----------

SELECT * FROM books;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * As we said, one of the main benefits of the merge operation is to avoid duplicates.
-- MAGIC * So, if we try to rerun the above statement again, it will not reinsert those records as they are already in the table.

-- COMMAND ----------

MERGE INTO books b
USING books_updates bu
ON b.book_id = bu.book_id AND b.title = bu.title
WHEN NOT MATCHED AND bu.category = 'Computer Science' THEN INSERT *;
