-- Databricks notebook source
-- Bronze: copia cruda de los datos, sin transformar
CREATE OR REPLACE TABLE workspace.taxi_demo.trips_bronze AS
SELECT * FROM samples.nyctaxi.trips;

-- COMMAND ----------

-- verifico
SELECT COUNT(*) AS total_rows FROM workspace.taxi_demo.trips_bronze;