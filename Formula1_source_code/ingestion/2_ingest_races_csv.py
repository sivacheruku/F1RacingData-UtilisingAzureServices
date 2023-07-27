# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.fs.ls('/mnt/formula1deltalke/raw')

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType
races_schema = StructType(fields = [
    StructField('raceId', IntegerType(), False),
    StructField('year', IntegerType(), True),
    StructField('round', IntegerType(), True),
    StructField('circuitId', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('date', DateType(), True),
    StructField('time', StringType(), True),
    StructField('url', StringType(), True),
])

# COMMAND ----------

#read races.csv
races_df = spark.read.option('header',True).schema(races_schema).csv(f'{raw_folder_path}/races.csv')
display(races_df)
races_df.printSchema()

# COMMAND ----------

renamed_cols_races_df = races_df.withColumnsRenamed({'raceId':'race_id','year':'race_year','circuitId':'circuit_id'}).drop('url')
display(renamed_cols_races_df)

# COMMAND ----------

# from pyspark.sql.functions import to_timestamp, concat, lit
# timestamps_added_races_df = renamed_cols_races_df.withColumns({
#     'race_timestamp': to_timestamp( concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'),
#     'ingestion_date' : current_timestamp()
# })

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit, col
timestamps_added_races_df = renamed_cols_races_df.withColumns({
    'race_timestamp': to_timestamp( concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'),
}).withColumn('data_source', lit(v_data_source))
final_df = add_ingestion_date(timestamps_added_races_df)

# COMMAND ----------

display(timestamps_added_races_df)

# COMMAND ----------

races_final_df = final_df.drop(col('date'),col('time'))
display(races_final_df)

# COMMAND ----------

# races_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.races')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1deltalke/processed/races'))

# COMMAND ----------

# MAGIC %md
# MAGIC # partition Races by year

# COMMAND ----------

# races_final_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_final_df.write.mode('overwrite').format('parquet').partitionBy('race_year').saveAsTable('f1_processed.races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1deltalke/processed/races

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")
