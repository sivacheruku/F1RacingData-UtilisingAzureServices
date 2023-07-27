-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

SELECT * FROM calculated_race_results;

-- COMMAND ----------

---All time constructor leaderboard
SELECT team_name,
      COUNT(*) AS no_of_games,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM calculated_race_results
GROUP BY team_name
HAVING no_of_games >= 100
ORDER BY average_points DESC;


-- COMMAND ----------

---Constructor leaderboard for this decade
SELECT team_name,
      COUNT(*) AS no_of_games,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING no_of_games >= 100
ORDER BY average_points DESC;

-- COMMAND ----------

---Constructor leaderboard for last decade
SELECT team_name,
      COUNT(*) AS no_of_games,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING no_of_games >= 100
ORDER BY average_points DESC;

-- COMMAND ----------


