# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

result = dbutils.notebook.run('1_ingest_circuits_csv', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('2_ingest_races_csv', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result= dbutils.notebook.run('3_ingest_constructors_json', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('4_ingest_drivers_json', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('5_ingest_results_json', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('6_ingest_pitstops_multiLine_json', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('7_ingest_lap_times_file', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)

# COMMAND ----------

result = dbutils.notebook.run('8_ingest_qualifying_multiLine_json_folder', 0, {'p_data_source_name': 'Ergast API'} )

# COMMAND ----------

print(result)
