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

# race_results_list = spark.read.parquet(f'{presentation_folder_path}/race_results') \
#     .filter(f"file_date = '{v_file_date}'") \
#     .select('race_year') \
#     .distinct() \
#     .collect()

# race_year_list =[]
# for race_year in race_results_list:
#     race_year_list.append(race_year.race_year)

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(f"file_date = '{v_file_date}'")

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results') \
    .filter(col('race_year').isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import count, when, desc, col, sum
team_scores_df = race_results_df.groupBy('race_year','team') \
                        .agg(sum('points').alias('total_points'), count(when(col("position") ==1 , True)).alias('wins'))

# COMMAND ----------

# display(team_scores_df.where('race_year = 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ranking based on points

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

ranking_window = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df = team_scores_df.withColumn('rank', rank().over(ranking_window))

# COMMAND ----------

# display(final_df.where('race_year = 2020'))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/constructor_standings')

# COMMAND ----------

# final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'constructor_standings','race_year')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(*) FROM f1_presentation.constructor_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;

# COMMAND ----------


