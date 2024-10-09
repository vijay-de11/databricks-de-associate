-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * In this notebook, we are going to explore advanced transformations present in Spark SQL

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM customers;

-- COMMAND ----------

DESCRIBE customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Spark SQL has built-in functionality to directly interact with JSON data stored as strings.
-- MAGIC * We can simply use the colon (:) syntax to traverse nested data structures.

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country FROM customers;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * Spark SQL also has the ability to parse JSON objects into struct types.
-- MAGIC * Struct is a native spark type with nested attributes. This can be done with **from_json()** function.
-- MAGIC * However if we run this, it will fail. Because this function requires the schema of the JSON object.
-- MAGIC * We can derive the schema from our current data. For this, we need a sample data of our JSON value with non null fields.
-- MAGIC * Now, we can copy the sample data and provide it to the **schema_of_json()** function
-- MAGIC * When we work with a struct type, it has the ability to interact with the nested object.

-- COMMAND ----------

SELECT from_json(profile) AS profile_struct
FROM customers;

-- COMMAND ----------

SELECT profile FROM customers LIMIT 1;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Susana","last_name":"Gonnely","gender":"Female","address":{"street":"760 Express Court","city":"Obrenovac","country":"Serbia"}}')) 
AS profile_struct
FROM customers;

SELECT * FROM parsed_customers;

-- COMMAND ----------

DESCRIBE parsed_customers;

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country FROM parsed_customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * With struct type, we can interact with the subfields using standard period or dot syntax instead of the colon syntax we use for the JSON string.
-- MAGIC * And once a json string is converted to a struct type, we can use the star operation to flatten fields into columns.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_flattened
AS SELECT customer_id, profile_struct.*
FROM parsed_customers;

SELECT * FROM customers_flattened;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## explode() function

-- COMMAND ----------

SELECT order_id, customer_id, books FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Here, books column is an **Array of Struct** type.
-- MAGIC * Spark SQL has number of functions specifically to deal with arrays.
-- MAGIC * **The most important one is the explode() function that allows us to put each element of an array on its own row.**
-- MAGIC * Now, each element of the book's array has its own row and we are repeating the other information like the order_id and the customer_id.
-- MAGIC

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS books FROM orders ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * **collect_set()** - aggregation function that allows us to collect unique values for a field, including fields within arrays.

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Can we flatten this array of array which is books_set column.
-- MAGIC * Yes, we can by using **array_distinct(flatten(collect_set()))**
-- MAGIC * With this, we will only have distinct values.

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) AS before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JOINS in Spark SQL
-- MAGIC * It supports all the standard JOINS.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_enriched AS
SELECT * FROM (
  SELECT *, explode(books) AS book
  FROM orders) o
INNER JOIN books b ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Spark SQL also supports set operations like UNION, INTERSECT, MINUS

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders
UNION
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders
INTERSECT
SELECT * FROM orders_updates

-- COMMAND ----------

SELECT * FROM orders
MINUS
SELECT * FROM orders_updates

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Spark SQL also supports **PIVOT** clause, which is used to change data perspective.
-- MAGIC * We can get the aggregated values based on a specific column values, which will be turned to multiple columns used in Select clause.
-- MAGIC * The pivot table can be specified after the table name or subquery.
-- MAGIC

-- COMMAND ----------

CREATE
OR REPLACE TABLE transactions AS
SELECT
  *
FROM
  (
    SELECT
      customer_id,
      book.book_id AS book_id,
      book.quantity AS quantity
    FROM
      orders_enriched
  ) PIVOT (
    SUM(quantity) FOR book_id IN (
      'B01',
      'B02',
      'B03',
      'B04',
      'B05',
      'B06',
      'B07',
      'B08',
      'B09',
      'B10',
      'B11',
      'B12'
    )
  );
SELECT
  *
FROM
  transactions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * So here we used the **Pivot** command to create a new transactions table that flatten out the information contained in the orders table for each customer.
-- MAGIC * Such a flatten data format can be useful for dashboarding, but also useful for applying ML algorithms for interface and predictions.
