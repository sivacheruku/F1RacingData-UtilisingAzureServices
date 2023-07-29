-- Databricks notebook source
---Drop existing databases and create new ones
DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
  LOCATION '/mnt/formula1deltalke/processed'

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
  LOCATION '/mnt/formula1deltalke/presentation'

-- COMMAND ----------


