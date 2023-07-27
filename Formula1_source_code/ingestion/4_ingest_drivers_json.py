# Databricks notebook source
# MAGIC %md
# MAGIC ##### the 'name' column of drivers data is in nested json format

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DateType, StructField, StructType

# COMMAND ----------

#define inner json object for 'name'
name_schema = StructType(fields = [
    StructField('forename', StringType(), True),
    StructField('surname', StringType(), True)
])

# COMMAND ----------

#wrap name_schema onto drivers schema
drivers_schema = StructType(fields = [
    StructField('driverId', IntegerType(), False),
    StructField('driverRef', StringType(), True),
    StructField('number', IntegerType(), True),
    StructField('code', StringType(), True),
    StructField('name', name_schema),
    StructField('dob', DateType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema) \
    .json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

drivers_processed_df = drivers_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit
drivers_final_df = drivers_processed_df.withColumnsRenamed({'driverId': 'driver_id', 'driverRef':'driver_ref'}) \
    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname'))) \
        .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

drivers_final = add_ingestion_date(drivers_final_df)

# COMMAND ----------

# drivers_final.write \
#     .mode('overwrite') \
#     .parquet(f'{processed_folder_path}/drivers/')

# COMMAND ----------

drivers_final.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.drivers')

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1deltalke/processed/drivers/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")
