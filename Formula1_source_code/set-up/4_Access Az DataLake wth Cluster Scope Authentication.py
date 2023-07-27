# Databricks notebook source
# MAGIC %md
# MAGIC ##Accessing data from Azure Data Lake storage (gen2) Cluster Scope Authentication
# MAGIC 1. Set the spark config fs.azure.account.key in cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Edited the cluster configuration with the spark configuration in a name-value pair setup
# MAGIC 1. name: fs.azure.account.key.formula1deltalke.dfs.core.windows.net
# MAGIC 1. value: GThgPm6gppRmL8ErJxYuY1LVD9BNrThxROj2EH9+q8pCnwGEnrbPwMsAr0h4wqMiqc7hB/hGFhw2+ASt64f1eA==

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1deltalke.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalke.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


