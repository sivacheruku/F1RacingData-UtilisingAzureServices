# Databricks notebook source
# MAGIC %md
# MAGIC #### Qualifying is a multi-line json file folder

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

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
    .json(f'{raw_folder_path}/qualifying')

# COMMAND ----------

display(qualifying_df.count())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
qualifying_final_df = qualifying_df \
    .withColumnsRenamed({'qualifyId':'qualify_id','raceId':'race_id','driverId':'driver_id', 'constructorId':'constructor_id'}) \
    .withColumn('data-source', lit(v_data_source))

# COMMAND ----------

qualifying_final = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# qualifying_final.write \
#     .mode('overwrite') \
#     .parquet(f'{processed_folder_path}/qualifying')

# COMMAND ----------

qualifying_final.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1delatalke/processed/qualifying'))

# COMMAND ----------

dbutils.notebook.exit("Success")
