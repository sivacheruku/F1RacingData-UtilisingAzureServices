# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.printSchema()

# COMMAND ----------

demo_df = spark.read.parquet(f'{presentation_folder_path}/race_results').where('race_year = 2020')

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'" ).select(countDistinct('race_name'), sum('points').alias('yoloo')).show()

# COMMAND ----------

demo_df.groupBy('driver_name') \
    .agg(sum('points').alias('Total Points'), countDistinct('race_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #window functions

# COMMAND ----------

demo_df = spark.read.parquet(f'{presentation_folder_path}/race_results').where('race_year = 2020 or race_year = 2019')

# COMMAND ----------

df = demo_df.groupBy('race_year','driver_name') \
    .agg(sum('points').alias('total_points'), countDistinct('race_name').alias('total_races'))

# COMMAND ----------

from pyspark.sql.functions import desc
df.orderBy('race_year', desc('total_points') ).show()

# COMMAND ----------

#create window
from pyspark.sql.window import Window
from pyspark.sql.functions import rank
driverrank_window = Window.partitionBy('race_year').orderBy(desc('total_points'))
df.withColumn('rank', rank().over(driverrank_window)).show()

# COMMAND ----------


