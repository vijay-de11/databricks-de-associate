-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Higher Order Functions
-- MAGIC * As we can see that books column is of complex data type which is Array of struct type.
-- MAGIC * To work directly with complex data type, we need to use **higher order functions**
-- MAGIC * HOF allows us to work directly with hierarchical data like arrays and map type objects.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Filter() function

-- COMMAND ----------

-- Filter the books having quantity >= 2
SELECT
  order_id,
  books,
  FILTER(books, i -> i.quantity >= 2) AS multiple_copies
FROM orders;

-- COMMAND ----------

SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER(books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders
)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. TRANSFORM()
-- MAGIC * Transform function is used to apply a transformation on all the items in an array and extract the transformed value.
-- MAGIC * In the below example, for each book in the books array, we are applying a discount on the subtotal value

-- COMMAND ----------

SELECT
  order_id,
  books,
  TRANSFORM(
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## User Defined Functions (UDF)
-- MAGIC * It allows us to register a custom combination of SQL logic as function in a database, making these methods reusable in any SQL query.
-- MAGIC * In addition, UDF functions leverage spark SQL directly maintaining all the optimization of Spark when applying our custom logic to large datasets.
-- MAGIC * At minimum, it requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC * Note that, user defined functions are permanent objects that are persisted to the database, so you can use them between different Spark sessions and notebooks.
-- MAGIC * With **DESCRIBE FUNCTION** command, we can see where it was registered and basic information about expected inputs and the expected return type.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers;

-- COMMAND ----------

DESCRIBE FUNCTION get_url;

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED get_url; -- It also tells us the logic used to create the UDF

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Everything is evaluated natively in Spark. And so its optimized for parallel execution.

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
