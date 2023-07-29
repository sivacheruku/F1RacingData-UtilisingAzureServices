# Databricks notebook source
# MAGIC %md
# MAGIC #### Qualifying is a multi-line json file folder

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

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

qualifying_schema = StructType(fields = [
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('position', IntegerType(), False),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option('multiline', True) \
    .json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

display(qualifying_df.count())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
qualifying_final_df = qualifying_df \
    .withColumnsRenamed({'qualifyId':'qualify_id','raceId':'race_id','driverId':'driver_id', 'constructorId':'constructor_id'}) \
    .withColumn('data-source', lit(v_data_source))\
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

qualifying_final = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# display(qualifying_final_df)

# COMMAND ----------

# qualifying_final.write \
#     .mode('overwrite') \
#     .parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

# qualifying_final.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

# overwrite_partition(qualifying_final, 'f1_processed', 'qualifying','race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(*) FROM f1_processed.qualifying
# MAGIC   GROUP BY race_id
# MAGIC   ORDER BY race_id DESC;

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1delatalke/processed/qualifying'))

# COMMAND ----------

dbutils.notebook.exit("Success")
