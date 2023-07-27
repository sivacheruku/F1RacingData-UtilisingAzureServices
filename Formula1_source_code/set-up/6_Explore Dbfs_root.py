# Databricks notebook source
# MAGIC %md
# MAGIC #Explore DBFS Root
# MAGIC 1. List all files in DBFS root
# MAGIC 1. Interact with DBFS file browser
# MAGIC 1. Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


