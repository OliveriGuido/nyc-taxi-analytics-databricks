-- Databricks notebook source
-- Silver: limpio viajes inválidos y outliers absurdos 
CREATE OR REPLACE TABLE workspace.taxi_demo.trips_silver AS
SELECT
  tpep_pickup_datetime  AS pickup_ts,
  tpep_dropoff_datetime AS dropoff_ts,
  DATE(tpep_pickup_datetime) AS pickup_date,
  HOUR(tpep_pickup_datetime) AS pickup_hour,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  -- duración del viaje en minutos
  (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60.0 AS trip_duration_min
FROM workspace.taxi_demo.trips_bronze
WHERE 
  trip_distance > 0           -- filtrar viajes inválidos
  AND fare_amount > 0
  AND fare_amount < 500       -- filtrar outliers absurdos
  AND tpep_pickup_datetime < tpep_dropoff_datetime; -- condición lógica 


-- COMMAND ----------

-- Verificar que las agregaciones dan razonables post sacar outliers
SELECT 
  COUNT(*) AS rows_silver,
  MIN(pickup_date) AS desde,
  MAX(pickup_date) AS hasta
FROM workspace.taxi_demo.trips_silver;
