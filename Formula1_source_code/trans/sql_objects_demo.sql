-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. create Database demo
-- MAGIC 1. data tab in UI
-- MAGIC 1. SHOW command
-- MAGIC 1. DESCRIBE command
-- MAGIC 1. find current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED DEMO;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

USE demo;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Managed Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_py')

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESCRIBE EXTENDED RACE_RESULTS_PY

-- COMMAND ----------

SELECT * FROM
      race_results_py
        WHERE race_year = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC create managed table using SQL

-- COMMAND ----------

CREATE TABLE race_results_sql AS 
  SELECT * FROM
      race_results_py
        WHERE race_year = 2020

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### When you drop a Managed table, you drop all the data within that table. Hive Meta Store will be empty after dropping the table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #External Table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').option('path', f'{presentation_folder_path}/race_results_ext_py').saveAsTable('demo.race_results_ext_py')

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

--USING SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql as
SELECT * FROM
      race_results_py
        WHERE race_year = 2020

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

drop table demo.race_results_ext_sql

-- COMMAND ----------

drop table demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Views on tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results 
AS
SELECT * FROM demo.race_results_py
  WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results 
AS
SELECT * FROM demo.race_results_py
  WHERE race_year = 2012

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #creating permanent view

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results 
AS
SELECT * FROM demo.race_results_py
  WHERE race_year = 2000

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP VIEW v_race_results

-- COMMAND ----------


