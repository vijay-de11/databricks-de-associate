# Databricks notebook source
# MAGIC %md
# MAGIC - In this, we are going to see how to orchestrate jobs with Databricks.
# MAGIC - Databricks allows us to schedule one or multiple tasks as part of the job.
# MAGIC - We are going to create a multi task job consists of 3 tasks.
# MAGIC   - First executing a notebook that lands a new batch of data in our source directory.
# MAGIC   - Then running our DLT pipeline created last session to process this data through a series of DLT tables.
# MAGIC   - Lastly, executing the notebook we created in the last session to show the pipeline results.
# MAGIC
# MAGIC - Create a new job from Workflows.
# MAGIC - For development, create an All-Purpose Cluster, but for prod always use Job Compute cluster for cost saving.
# MAGIC - While configuring the job, we can also configuring the scheduling on a need basis whether to continuously trigger the job or on a scheduled basis.
# MAGIC - We can also configure notifications by providing an email.
# MAGIC - From the Permission secition, we can control who can Run, Manage or View the jobs. Either a user or group of users.
# MAGIC - We can provide an Is Owner privilege to a user but not to group of users.
