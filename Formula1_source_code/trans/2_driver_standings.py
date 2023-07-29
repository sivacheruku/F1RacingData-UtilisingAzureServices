# Databricks notebook source
dbutils.widgets.text('p_file_date', '2021-03-21')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Find race years for which the data is to be processed

# COMMAND ----------

race_results_list = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(f"file_date = '{v_file_date}'") \
    .select('race_year') \
    .distinct() \
    .collect()

# COMMAND ----------

race_year_list =[]
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, desc
driver_standings = race_results_df.groupBy('race_year', 'driver_name', 'driver_nationality', 'team') \
    .agg(sum('points').alias('total_points'), count(when(race_results_df.position == 1, True)).alias('wins'))

# COMMAND ----------

display(driver_standings.where('race_year = 2020').orderBy(desc('total_points')))

# COMMAND ----------

# MAGIC %md
# MAGIC for rank

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank


driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'), desc('wins'))
final_df = driver_standings.withColumn('rank', rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.where('race_year = 2020'))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/driver_standings')

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.driver_standings')

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings','race_year')

# COMMAND ----------


