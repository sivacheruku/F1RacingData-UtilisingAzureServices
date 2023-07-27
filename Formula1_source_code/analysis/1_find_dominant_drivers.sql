-- Databricks notebook source
SELECT * FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

SELECT driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING no_of_races >= 50
  ORDER BY average_points DESC;

-- COMMAND ----------

--Drivers who dominated in this decade
SELECT driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
  GROUP BY driver_name
  HAVING no_of_races >= 50
  ORDER BY average_points DESC;

-- COMMAND ----------

--Drivers who dominated last decade
SELECT driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
  GROUP BY driver_name
  HAVING no_of_races >= 50
  ORDER BY average_points DESC;

-- COMMAND ----------


