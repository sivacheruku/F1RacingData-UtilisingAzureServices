-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """ <h1 style ="color:Black;text-align:center;font-family:Ariel"> Report on Dominant Formula1 Drivers </h1> """
-- MAGIC displayHTML(html)

-- COMMAND ----------

--Add Rank
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points,
      RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
  FROM f1_presentation.calculated_race_results
  GROUP BY driver_name
  HAVING no_of_races >= 50
  ORDER BY average_points DESC;

-- COMMAND ----------

--Add Rank over years
SELECT race_year,
      driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name 
                          FROM v_dominant_drivers 
                          WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, average_points DESC;

-- COMMAND ----------

--Add Rank over years
SELECT race_year,
      driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name 
                          FROM v_dominant_drivers 
                          WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, average_points DESC;

-- COMMAND ----------

--Add Rank over years
SELECT race_year,
      driver_name,
      COUNT(*) AS no_of_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM f1_presentation.calculated_race_results
    WHERE driver_name IN (SELECT driver_name 
                          FROM v_dominant_drivers 
                          WHERE driver_rank <= 10)
  GROUP BY race_year, driver_name
  ORDER BY race_year, average_points DESC;

-- COMMAND ----------


