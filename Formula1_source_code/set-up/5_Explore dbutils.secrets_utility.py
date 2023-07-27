# Databricks notebook source
# MAGIC %md
# MAGIC ###We will use dbutils.secrets to get imp KEYS stored in Azure Key Vault

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1scope')

# COMMAND ----------

dbutils.secrets.get('formula1scope', 'formula1dl-account-key')

# COMMAND ----------


