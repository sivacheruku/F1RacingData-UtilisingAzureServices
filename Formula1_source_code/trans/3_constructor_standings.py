# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

from pyspark.sql.functions import count, when, desc, col, sum
team_scores_df = race_results_df.groupBy('race_year','team') \
                        .agg(sum('points').alias('total_points'), count(when(col("position") ==1 , True)).alias('wins'))

# COMMAND ----------

display(team_scores_df.where('race_year = 2020'))

# COMMAND ----------

# MAGIC %md
# MAGIC ranking based on points

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

ranking_window = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
final_df = team_scores_df.withColumn('rank', rank().over(ranking_window))

# COMMAND ----------

display(final_df.where('race_year = 2020'))

# COMMAND ----------

# final_df.write.mode('overwrite').parquet(f'{presentation_folder_path}/constructor_standings')

# COMMAND ----------

final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.constructor_standings')
