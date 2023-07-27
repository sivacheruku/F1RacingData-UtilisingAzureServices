# Databricks notebook source
# MAGIC %md
# MAGIC # Accessing dataframes using SQL
# MAGIC 1. Create temporary views on dataframe
# MAGIC 1. Access views from SQL
# MAGIC 1. Access view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

race_results_df.createTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results where race_year=2020

# COMMAND ----------

race_results_2019 = spark.sql('select * from v_race_results where race_year = 2019')

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC #Global temperory view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------


