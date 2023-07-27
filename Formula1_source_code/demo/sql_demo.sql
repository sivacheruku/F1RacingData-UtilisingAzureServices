-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED drivers;

-- COMMAND ----------

SELECT * FROM drivers
  LIMIT 10;

-- COMMAND ----------

SELECT * FROM drivers
  WHERE nationality = 'British';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SQL Functions
-- MAGIC

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy') FROM drivers

-- COMMAND ----------


