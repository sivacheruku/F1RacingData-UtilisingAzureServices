# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times folder

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

lap_times_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), True),
    StructField('lap', IntegerType(), True),
    StructField('position', IntegerType(), True),
    StructField('time', StringType(), True),
    StructField('milliseconds', IntegerType(), True)
])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_folder_path}/lap_times')

# or .csv('/mnt/formula1deltalke/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

display(lap_times_df.count())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
lap_times_final_df = lap_times_df \
    .withColumnsRenamed({'raceId':'race_id','driverId':'driver_id'}) \
    .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

lap_times = add_ingestion_date(lap_times_final_df)

# COMMAND ----------

display(lap_times)

# COMMAND ----------

# lap_times.write \
#     .mode('overwrite') \
#     .parquet('/mnt/formula1delatalke/processed/lap_times')

# COMMAND ----------

lap_times.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times;

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1delatalke/processed/lap_times'))

# COMMAND ----------

dbutils.notebook.exit("Success")