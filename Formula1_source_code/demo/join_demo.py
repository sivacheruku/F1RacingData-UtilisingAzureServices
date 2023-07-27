# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')
races_df = spark.read.parquet(f'{processed_folder_path}/races').where('race_year = 2019')

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

#df to join these two

# COMMAND ----------

from pyspark.sql.functions import expr
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner')\
    .select(circuits_df.name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------


