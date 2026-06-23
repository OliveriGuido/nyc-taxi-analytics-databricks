-- Databricks notebook source
SELECT * FROM samples.nyctaxi.trips LIMIT 10;

-- COMMAND ----------

-- todas las columnas y tipos
DESCRIBE samples.nyctaxi.trips;

-- COMMAND ----------

SELECT COUNT(*) FROM samples.nyctaxi.trips;

-- COMMAND ----------

-- crear esquema medallion -> tablas Bronze / Silver / Gold 
CREATE SCHEMA IF NOT EXISTS workspace.taxi_demo;