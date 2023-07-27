-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """ <h1 style ="color:Black;text-align:center;font-family:Ariel"> Report on Dominant Formula1 Teams </h1> """
-- MAGIC displayHTML(html)

-- COMMAND ----------

---All time constructor leaderboard with ranks (create a view of them)
CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
      COUNT(*) AS no_of_games,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points,
      RANK() OVER( ORDER BY AVG(calculated_points) DESC) AS team_rank
  FROM calculated_race_results
GROUP BY team_name
HAVING no_of_games >= 100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT * FROM v_dominant_teams

-- COMMAND ----------

--Observe the leaderboard member performance over the years

SELECT race_year,
      team_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM calculated_race_results
  WHERE team_name IN ( SELECT team_name FROM v_dominant_teams
                        WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------

--Observe the leaderboard member performance over the years

SELECT race_year,
      team_name,
      COUNT(*) AS total_races,
      SUM(calculated_points) AS total_points,
      AVG(calculated_points) AS average_points
  FROM calculated_race_results
  WHERE team_name IN ( SELECT team_name FROM v_dominant_teams
                        WHERE team_rank <= 5)
GROUP BY race_year, team_name
ORDER BY race_year, average_points DESC;

-- COMMAND ----------


