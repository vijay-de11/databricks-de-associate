# Databricks notebook source
# MAGIC %md
# MAGIC * In this notebook, we will explore the basics of working with Spark Structured Streaming to allow incremental processing of data.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
      )

# COMMAND ----------

# MAGIC %md
# MAGIC * To work with data streaming in SQL, you must first use **spark.readStream** method in Pyspark API.
# MAGIC * spark.readStream method allows to query a Delta table as a stream source. 
# MAGIC * And from there, we can register a temporary view against this stream source.
# MAGIC * The temporary view created here is a "streaming" temporary view that allows to apply most transformations in SQL the same way as we would with the static data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC * When we try to retrieve data from streaming_df, we see that the query is still running, waiting for any new data to be displayed here.
# MAGIC * Generally speaking, we don't display a streaming result unless a human is actively monitoring the output of a query during development or live dashboarding.
# MAGIC * We need to click Interrupt to stop running the streaming query.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author;

# COMMAND ----------

# MAGIC %md
# MAGIC * Again, because we are querying a streaming temporary view, this becomes a streaming query that executes infinitely, rather than completing after retrieving a single set of results.
# MAGIC * We are just displaying an aggregation of input as seen by the stream. None of these records are being persisted anywhere at this point.
# MAGIC * For streaming queries like this, we can always explore an  interactive dashboard to monitor the streaming performance.
# MAGIC * **REMEMBER** : When working with streaming data, some operations are not supported like **sorting**
# MAGIC * However, we can use advanced methods like **windowing** and **watermarking** to achieve such operations, but it is out of scope for this course.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM books_streaming_tmp_vw
# MAGIC ORDER BY author;

# COMMAND ----------

# MAGIC %md
# MAGIC * In order to persist incremental results, we need to first pass our logic back to Pyspark Dataframe API.
# MAGIC * If we create a temporary view out of a spark streaming temporary view then the created temporary view is also called as streaming temporary view.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS
# MAGIC (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

(
    spark.table("author_counts_tmp_vw")
         .writeStream
         .trigger(processingTime="4 seconds")
         .outputMode("complete")
         .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
         .table("author_counts")
)

# COMMAND ----------

# MAGIC %md
# MAGIC * Note that spark always loads streaming views as a streaming DataFrames, and static views as a static DataFrames.
# MAGIC * That means, incremental processing must be defined from the very beginning with Read logic to support later an incremental writing.
# MAGIC * For aggregation streaming queries, we must always use "complete" mode to overwrite the table with the new location.
# MAGIC * The checkpointLocation is used to help tracking the progress of the streaming processing.
# MAGIC * We can think about such a streaming query as an always-on incremental query, and we can always explore its interactive dashboard.
# MAGIC * From this dashboard, we can see that the data has been processed and we can now query our target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;

# COMMAND ----------

# MAGIC %md
# MAGIC * When we execute a streaming query, the streaming query will continue to update as new data arrives in the source.
# MAGIC * To confirm this, let us add some new data to the books table.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding new data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC        ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC        ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35);

# COMMAND ----------

# MAGIC %md
# MAGIC * Cancel the above streaming query to see another scenario.
# MAGIC * Always remember to cancel any active stream in your notebook, otherwise the stream will be always on and prevents the cluster from auto termination.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming in batch mode

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC        ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC        ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC * In the below scenario, we modify the trigger method to change our query from an always-on query triggered every 4 seconds to a triggered incremental batch.
# MAGIC * We do this using the **availableNow** trigger option. With this trigger option, the query will process all new available data and stop on its own after execution.
# MAGIC * In this case, we can use the **awaitTermination** method to block the execution of any cell in this notebook until the incremental batch's write has succeeded.

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts;
