-- Databricks notebook source
CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create "CLASSICAL/STORED" view that shows only Apple phones

-- COMMAND ----------

CREATE VIEW view_apple_phones AS
SELECT * FROM smartphones WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC * Remember, each time a **view** is queried, the underlying logical query of the view is executed against the table.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a "TEMPORARY" view for distinct smartphones brands

-- COMMAND ----------

CREATE TEMPORARY VIEW temp_view_phones_brand
AS SELECT DISTINCT brand FROM smartphones;

-- COMMAND ----------

SELECT * FROM temp_view_phones_brand;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * Since, temp_view_phones_brand is a **temp view** therefore it is not persisted in the database.

-- COMMAND ----------

-- MAGIC %md ### Create a "GLOBAL TEMP VIEW" when year of manufacturing is > 2020

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
WHERE year > 2020 ORDER BY year DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * To query a global temp view, we need to use the **global_temp** keyword before view
-- MAGIC * SELECT * FROM global_temp.global_temp_view_name

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * Since, global temp view is tied to the global_temp database, we use the command **SHOW TABLES IN global_temp** to see the global_temp database tables.

-- COMMAND ----------

SHOW TABLES IN global_temp;
