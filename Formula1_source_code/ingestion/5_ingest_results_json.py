# Databricks notebook source
# MAGIC %md
# MAGIC 1. Ingesting the results.json file
# MAGIC 1. We follow the normal steps as previous using a Dataframe json reader API and writer API

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1deltalke/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###### create schema for results.json

# COMMAND ----------

from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType

# COMMAND ----------

#refer schema using StructType
results_schema = StructType( fields= [
    StructField('resultId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), True),
    StructField('grid', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('positionText', StringType(), False),
    StructField('positionOrder', IntegerType(), False),
    StructField('points', FloatType(), False),
    StructField('laps', IntegerType(), False),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True),
    StructField('fastestLap', IntegerType(), True),
    StructField('rank', IntegerType(), True),
    StructField('fastestLapTime', StringType(), True),
    StructField('fastestLapSpeed', FloatType(), True),
    StructField('statusId', StringType(), False)
])

# COMMAND ----------

results_df = spark.read\
                .schema(results_schema) \
                .json(f'{raw_folder_path}/results.json')

# COMMAND ----------

display(results_df)
display(results_df.printSchema())

# COMMAND ----------

results_df_processed = results_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
results_final_df = results_df_processed \
    .withColumnsRenamed({'resultId':'result_id', 'raceId':'race_id', 'driverId': 'driver_id', 'constructorId': 'constructor_id', 'positionText': 'position_text', 'positionOrder': 'position_order', 'fastestLap':'fastest_lap','fastestlapTime':'fastest_lap_time','fastestLapSpeed':'fastest_lap_speed'})
results = results_final_df.withColumn('data_source', lit(v_data_source))

# COMMAND ----------

results_final = add_ingestion_date(results)

# COMMAND ----------

display(results_final)

# COMMAND ----------

# results_final.write \
#     .mode('overwrite') \
#     .partitionBy('race_id') \
#     .parquet(f'{processed_folder_path}/results')

# COMMAND ----------

results_final.write.mode('overwrite').format('parquet').partitionBy('race_id').saveAsTable('f1_processed.results')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1deltalke/processed/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.results

# COMMAND ----------

dbutils.notebook.exit("Success")
