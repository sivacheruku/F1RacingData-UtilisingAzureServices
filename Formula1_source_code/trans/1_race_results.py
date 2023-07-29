# Databricks notebook source
# display(dbutils.fs.ls('/mnt/formula1deltalke/processed'))

# COMMAND ----------

# dbutils.fs.help()

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

def read_df_from_processed(processed_folder_path, folder_name):
    return spark.read.parquet(f'{processed_folder_path}/{folder_name}')

# COMMAND ----------

races_df = read_df_from_processed(processed_folder_path, 'races').withColumnRenamed('name','race_name')
circuits_df = read_df_from_processed(processed_folder_path, 'circuits').withColumnsRenamed({'name':'circuit_name','location':'circuit_location'})
drivers_df = read_df_from_processed(processed_folder_path, 'drivers').withColumnsRenamed({'name':'driver_name', 'nationality':'driver_nationality','number':'driver_number'})
constructors_df = read_df_from_processed(processed_folder_path, 'constructors').withColumnRenamed('name','constructor_name')


# COMMAND ----------

results_df = read_df_from_processed(processed_folder_path, 'results').filter(f"file_date = '{v_file_date}'")\
.withColumnRenamed('race_id', 'results_race_id') \
.withColumnRenamed('file_date', 'results_file_date')

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, 'left')

# COMMAND ----------

presentation_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, 'left') \
          .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, 'left') \
          .join(races_circuits_df, results_df.results_race_id == races_circuits_df.race_id, 'left')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

final_df = presentation_df.selectExpr("results_race_id as race_id", "race_year","race_name", "race_timestamp as race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality","constructor_name as team", "grid","fastest_lap","time", "points", 'position', 'results_file_date as file_date') \
    .withColumn('created_date', current_timestamp())

# COMMAND ----------

display(final_df.filter("race_name = 'Abu Dhabi Grand Prix' and race_year = 2020").orderBy('points', ascending = False))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'race_results','race_id')

# COMMAND ----------


