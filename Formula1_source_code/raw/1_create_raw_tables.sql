-- Databricks notebook source
-- MAGIC %md
-- MAGIC create external tables for all our data tables

-- COMMAND ----------

-- CREATE A NEW DATABASE TO STORE THE RAW TABLES
CREATE DATABASE
  IF NOT EXISTS f1_raw;

-- COMMAND ----------

SELECT current_database();
USE f1_raw;

-- COMMAND ----------

DESC DATABASE f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC So, the location of our database will be in the default storage, i.e hive storage of databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt DOUBLE,
  url DOUBLE
)
USING csv
OPTIONS (path '/mnt/formula1deltalke/raw/circuits.csv', header True)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Races table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS(path '/mnt/formula1deltalke/raw/races.csv', header True)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Constructors table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path '/mnt/formula1deltalke/raw/constructors.json')

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Create Drivers table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path '/mnt/formula1deltalke/raw/drivers.json')

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Results table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points FLOAT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING JSON
OPTIONS(path '/mnt/formula1deltalke/raw/results.json')

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create pit_stops table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS(path '/mnt/formula1deltalke/raw/pit_stops.json', multiLine True)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### create Lap times table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING csv
OPTIONS(path '/mnt/formula1deltalke/raw/lap_times')

-- COMMAND ----------

SELECT * FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Qualifying table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING json
OPTIONS(path '/mnt/formula1deltalke/raw/qualifying', multiLine True)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------


