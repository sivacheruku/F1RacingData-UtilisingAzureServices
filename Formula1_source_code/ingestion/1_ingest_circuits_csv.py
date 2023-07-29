# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text('p_data_source_name', '')
v_data_source = dbutils.widgets.get('p_data_source_name')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

#get configuration information from a different notebook using %run

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

#checking whether the %run worked or not
print(raw_folder_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step-1 Read the CSV file using the spark dataframe reader

# COMMAND ----------

# circuits_df = spark.read.option('header', True).option('inferschema', True).csv('dbfs:/mnt/formula1deltalke/raw/circuits.csv')
#we want to use our own schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

circuits_schema = StructType(fields = [
    StructField('circuitId', IntegerType(), False),
    StructField('circuitRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('location', StringType(), True),
    StructField('country', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('lng', DoubleType(), True),
    StructField('alt', DoubleType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read.option('header', True) \
    .schema(circuits_schema) \
    .csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md 
# MAGIC --------- selecting specific columns

# COMMAND ----------

from pyspark.sql.functions import col
selected_cols_circuits_df = circuits_df.select(col('circuitId'), col('circuitRef'), col('name'), col('location'), col('country'), col('lat'), col('lng'), col('alt'))

# COMMAND ----------

# MAGIC %md
# MAGIC ---------renaming the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit
renamed_cols_circuits_df = selected_cols_circuits_df.withColumnsRenamed({'circuitId':'circuit_id', 'circuitRef':'circuit_ref','lat':'latitude','lng':'longitude','alt':'altitude'}) \
    .withColumn('data_source', lit(v_data_source)) \
    .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ---add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
#circuits_final_df = renamed_cols_circuits_df.withColumn('ingestion_date',current_timestamp())
circuits_final_df = add_ingestion_date(renamed_cols_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ----- Write our final data into Data lake (file system)

# COMMAND ----------

# circuits_final_df.write.mode('overwrite').parquet(f'{processed_folder_path}/circuits')

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.circuits')

# COMMAND ----------

display(spark.read.parquet(f'/mnt/formula1deltalke/processed/circuits'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits;

# COMMAND ----------

dbutils.notebook.exit("Success")
