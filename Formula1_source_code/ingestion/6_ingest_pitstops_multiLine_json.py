# Databricks notebook source
# MAGIC %md
# MAGIC #### Pitstops is a multi-line json file

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

# print(processed_folder_path)

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

pit_stops_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('stop', StringType(), True),
    StructField('lap', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('duration', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option('multiline', True) \
    .json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
pit_stops_final_df = pit_stops_df.withColumnsRenamed({'raceId':'race_id','driverId':'driver_id'}).withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

pit_stops_new = add_ingestion_date(pit_stops_final_df)

# COMMAND ----------

display(pit_stops_new)

# COMMAND ----------

# pit_stops_new.write.mode('overwrite').parquet(f'{processed_folder_path}/pit_stops/')

# COMMAND ----------

# pit_stops_new.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

# overwrite_partition(pit_stops_new, 'f1_processed', 'pit_stops','race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stops_new, 'f1_processed', 'pit_stops', '/mnt/formula1deltalke/processed', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.pit_stops
# MAGIC     GROUP BY race_id
# MAGIC     ORDER BY race_id DESC;

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1delatalke/processed/pit_stops'))

# COMMAND ----------

dbutils.notebook.exit("Success")
