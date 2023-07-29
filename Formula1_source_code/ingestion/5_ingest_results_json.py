# Databricks notebook source
# MAGIC %md
# MAGIC 1. Ingesting the results.json file
# MAGIC 1. We follow the normal steps as previous using a Dataframe json reader API and writer API

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-28')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
                .json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

results_df_processed = results_df.drop('statusId')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
results_final_df = results_df_processed \
    .withColumnsRenamed({'resultId':'result_id', 'raceId':'race_id', 'driverId': 'driver_id', 'constructorId': 'constructor_id', 'positionText': 'position_text', 'positionOrder': 'position_order', 'fastestLap':'fastest_lap','fastestlapTime':'fastest_lap_time','fastestLapSpeed':'fastest_lap_speed'}) 
results = results_final_df.withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

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

# MAGIC %md
# MAGIC # Method 1

# COMMAND ----------

# #collecting distinct race_ids into a list so, that we can loop through it and do ALTER TABLE
# for race_id_list in results_final.select('race_id').distinct().collect():
#     # print(race_id_list)
#     if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f'ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})')

# COMMAND ----------

# results_final.write.mode('append').format('parquet').partitionBy('race_id').saveAsTable('f1_processed.results')

# COMMAND ----------

#If we do a regular append & rerun a data that is already there, it will result in duplicate values
#We know data is partitioned by race_id. So, we can do one thing. We can delete the file with race_id and append

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results

# COMMAND ----------

# #We need race_id to be the last column
# results_final_new = results_final.select('result_id','driver_id','constructor_id','number','grid','position','position_text','position_order','points','laps','time','milliseconds','fastest_lap','rank','fastest_lap_time','fastest_lap_speed','data_source','file_date','ingestion_date','race_id')

#We created a function to do the same process (re_arrange_partition_column)

# COMMAND ----------

# We created a function that overwrites the files depending on the requirement

# COMMAND ----------

overwrite_partition(results_final, 'f1_processed', 'results','race_id')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1deltalke/processed/results'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.results
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------


