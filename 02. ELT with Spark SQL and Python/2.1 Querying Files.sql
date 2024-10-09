-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT COUNT(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * When reading multiple files, it is useful to add the input_file_name function, which is a built-in Spark SQL command that records the source data file for each record.
-- MAGIC * This can be especially helpful if troubleshooting problems in the source data become necessary.

-- COMMAND ----------

SELECT *, input_file_name() AS source_file FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Another option here is to use the **text_format**, which allows us to query any text based files like JSON, CSV, TSV, or TXT format.

-- COMMAND ----------

SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * If we use text format to load the data, then we see that it loads each line of the file as a row with one string column naed **value**
-- MAGIC * This can be useful, when data could be corrupted. And we need to use some custom text parsing function to extract data.

-- COMMAND ----------

SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * In addition, we can use **binaryFile** to extract the raw bytes and some metadata of files.
-- MAGIC * It gives us the **path, modificationTime, length and the content in binary representation of the file**.

-- COMMAND ----------

SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Although, we have parsed the csv, but the header row is being extracted as a table row and all columns are being loaded in a single column.
-- MAGIC * And it seems that this is because of the delimiter of the file, which is ; (semicolon)
-- MAGIC * Infact, querying files in this way works well only with self-describing formats. The formats that have well-defined schema like JSON and Parquet. However, for other formats like CSV where there is no schema defined, this doesn't work and we need another way that allows us to provide additional configuration and schema declaration.
-- MAGIC
-- MAGIC | Solution |
-- MAGIC * One solution is to create a table with the **USING** keyword.
-- MAGIC * This allows us to create a table against an external source like CSV format. So here, we need to specify the **table schema** (i.e. the column name and types), the **file format** which is CSV. And whether if there is a **header** in the source files, and the **delimiter** used to separate fields which is ; (semicolon). And finally, we need to specify the location to the files directory.
-- MAGIC
-- MAGIC **!! Remember, when working with CSV files as data source, it is important to ensure that column order doesn't change if additional data files will be added to the source directory. !!**
-- MAGIC
-- MAGIC * Spark will always load data and apply column names and data types in the order specified during table creation.
-- MAGIC

-- COMMAND ----------

CREATE TABLE books_csv
(book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";" 
)
LOCATION '${dataset.bookstore}/books-csv'

-- COMMAND ----------

SELECT * FROM books_csv;

-- COMMAND ----------

DESCRIBE EXTENDED books_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Its a table that's referring directly to the CSV files. It means that no data has moved during table creation.
-- MAGIC * We are just pointing to files stored in an external location.
-- MAGIC * In addition, all the metadata and options passed during table creation will be persisted to the metadata ensuring that data in the above location will always be read with these options.
-- MAGIC * | Storage Properties | [delimiter=;, header=true] |

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * Let us now see the impact of not having a Delta table.
-- MAGIC * All the guarantess and features that works with Delta table will no longer work with external data sources like CSV.
-- MAGIC * For ex: Delta lake tables guarantee that you always query the most recent version of your source data, while tables registered against other data sources like CSV may represent older cached versions.
-- MAGIC * Let us add some new CSV file to our directory and see what will happen.
-- MAGIC * First, lets check how many CSV files we have in our directory
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC  . table("books_csv")
-- MAGIC  .write
-- MAGIC  .mode("append")
-- MAGIC  .format("csv")
-- MAGIC  .options(header="true", delimiter=";")
-- MAGIC  .save(f"{dataset_bookstore}/books-csv"))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Even with the new data has been successfully written to the table directory, we're still unable to see the new data.
-- MAGIC * And this is because Spark automatically cached the underlying data in local storage to ensure that for subsequent queries, Spark will provide the optimal performance by just querying this local cache.
-- MAGIC * This external CSV file is not configured to tell the Spark that it should refresh this data.
-- MAGIC * However, we can manually refresh the cache of our data by running the **REFRESH** table command.
-- MAGIC * But remember, refreshing a table will invalidate its cache, meaning that we will need to scan our original data source and pull all data back into memory. For a very large dataset, this may take a significant amount of time.
-- MAGIC * So, non-DELTA tables has these limitations.

-- COMMAND ----------

REFRESH TABLE books_csv;

-- COMMAND ----------

SELECT COUNT(*) FROM books_csv;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * To create Delta tables where we load data from external sources, we use CREATE TABLE AS SELECT STATEMENTS or CTAS statements.

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * We can see that the Delta table is created usin CTAS statment. 
-- MAGIC   * Provider - delta
-- MAGIC   * Type - MANAGED
-- MAGIC * In addition, we can see that schema has been inferred automatically from the query results. This is because CTAS statements automatically infer schema information from query results and do not support manual schema declaration.
-- MAGIC * This means that CTAS statements are useful for external data ingestion from sources with well-defined schema such as parquet files and tables.
-- MAGIC * In addition, CTAS statements do no support specifying additional file options which represents significant limitation when trying to ingest data from CSV files.

-- COMMAND ----------

CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC * Although, we have successfully created a Delta table, however the data is not well parsed.
-- MAGIC * To correct this, we need to first use a reference to the files that allows us to specify options.
-- MAGIC * We can acheive this by creating a temporary view that allows us to specify file options. Then we will use this temporary view as the source for our CTAS statement to successfully register the Delta table.

-- COMMAND ----------

CREATE TEMP VIEW books_temp_view
(book_id STRING, title  STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS(
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);

CREATE TABLE books AS
SELECT * FROM books_temp_view;

SELECT * FROM books;

-- COMMAND ----------

DESCRIBE EXTENDED books;
