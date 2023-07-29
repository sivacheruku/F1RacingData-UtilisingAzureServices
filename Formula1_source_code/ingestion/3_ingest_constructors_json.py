# Databricks notebook source
# MAGIC %md
# MAGIC 1. Ingesting the constructors.json file
# MAGIC 1. We follow the normal steps as previous using a Dataframe json reader API and writer API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1deltalke/raw

# COMMAND ----------

# MAGIC %md
# MAGIC ###### create schema for constructors.json

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# COMMAND ----------

#refer schema using StructType
constructors_schema = StructType( fields= [
    StructField('constructorId', IntegerType(), False),
    StructField('constructorRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

#refer schema using DDL method
constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read\
                .schema(constructors_schema) \
                .json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

# display(constructors_df)
# display(constructors_df.printSchema())

# COMMAND ----------

constructors_df_processed = constructors_df.drop('url')
# constructors_df_processed.printSchema

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp
# constructors_final_df = constructors_df_processed.withColumnsRenamed({'constructorId': 'constructor_id', 'constructorRef': 'constructor_ref'}) \
#                                                 .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
constructors_final_df = constructors_df_processed.withColumnsRenamed({'constructorId': 'constructor_id', 'constructorRef': 'constructor_ref'}) \
.withColumn('data_source', lit(v_data_source)) \
.withColumn('file_date', lit(v_file_date))

constructors_final_df2 = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# display(constructors_final_df)

# COMMAND ----------

# constructors_final_df2.write \
#     .mode('overwrite') \
#     .parquet(f'{processed_folder_path}/constructors/')

# COMMAND ----------

constructors_final_df2.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# display(spark.read.parquet('/mnt/formula1deltalke/processed/constructors/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")
