# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter('race_year > 2018 and race_year < 2021')

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


