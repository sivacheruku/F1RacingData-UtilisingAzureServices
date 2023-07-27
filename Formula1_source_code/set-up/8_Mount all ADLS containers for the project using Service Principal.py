# Databricks notebook source
# MAGIC %md
# MAGIC ##Mount Azure Data Lake storage (gen2) containers for the project
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    client_id = dbutils.secrets.get('formula1scope', 'formula1-client-id')
    tenant_id = dbutils.secrets.get('formula1scope', 'formula1-tenant-id')
    client_secret = dbutils.secrets.get('formula1scope', 'formula1-client-secret') 
    #set spark config
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}  
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
        
    #mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())
    return None

# COMMAND ----------

#mount Raw container
mount_adls('formula1deltalke','raw')

# COMMAND ----------

#mount presentation container
mount_adls('formula1deltalke','presentation')
#mount processed container
mount_adls('formula1deltalke','processed')

# COMMAND ----------

display(dbutils.fs.ls('/mnt/formula1deltalke/demo'))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1deltalke/demo/circuits.csv"))

# COMMAND ----------

#information about all mounts
display(dbutils.fs.mounts())

# COMMAND ----------

#unmounting an existing mount
dbutils.fs.unmount('/mnt/formula1deltalke/demo')
