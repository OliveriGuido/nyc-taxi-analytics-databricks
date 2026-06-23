-- Databricks notebook source
-- Gold: agregaciones listas para consumo de negocio
CREATE OR REPLACE TABLE workspace.taxi_demo.daily_metrics AS
SELECT
  pickup_date,
  COUNT(*) AS total_trips,
  ROUND(SUM(fare_amount), 2) AS total_revenue,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM workspace.taxi_demo.trips_silver
GROUP BY pickup_date
ORDER BY pickup_date;

-- COMMAND ----------

-- Segunda tabla gold: análisis por hora del día
CREATE OR REPLACE TABLE workspace.taxi_demo.hourly_patterns AS
SELECT
  pickup_hour,
  COUNT(*) AS total_trips,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM workspace.taxi_demo.trips_silver
GROUP BY pickup_hour
ORDER BY pickup_hour;