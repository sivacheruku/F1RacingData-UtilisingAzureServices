-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Here, we will create a database to hold our processed tables.
-- MAGIC 1. Unlike raw which is in default Hive storage, the location of our processed database will be inside our mount

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
  LOCATION '/mnt/formula1deltalke/processed'

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED f1_processed

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Since we have used SQL method to create tables in raw, we will use Pyspark method now
-- MAGIC 1. For doing that, we can edit all the initially ingested files in the folder "Ingested"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We just need to change the spark.write command we did in those notebooks
-- MAGIC 1. Old: circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')
-- MAGIC 1. New: circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')
