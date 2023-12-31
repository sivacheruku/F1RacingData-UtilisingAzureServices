# Databricks notebook source
# MAGIC %md
# MAGIC ##Accessing data from Azure Data Lake storage (gen2) with Service Principal
# MAGIC 1. Register Azure AD Application
# MAGIC 1. Create a secret/password for the application
# MAGIC 1. Set Spark Config with App/Client Id, Directory/Tenant Id
# MAGIC 1. Assign role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = "client_id_of_service_principal"
tenant_id = "tenant_id_of_service_principal"
client_secret = "client_secret_of_service_principal"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1deltalke.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1deltalke.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1deltalke.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1deltalke.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1deltalke.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls('abfss://demo@formula1deltalke.dfs.core.windows.net/'))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1deltalke.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


