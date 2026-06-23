-- Databricks notebook source
--Query 1: Top 10 zonas de pickup por revenue
SELECT 
  pickup_zip,
  COUNT(*) AS total_trips,
  ROUND(SUM(fare_amount), 2) AS total_revenue,
  ROUND(AVG(fare_amount), 2) AS avg_fare
FROM workspace.taxi_demo.trips_silver
GROUP BY pickup_zip
ORDER BY total_revenue DESC
LIMIT 10;

-- COMMAND ----------

--Query 2: Ranking de horas más rentables por día de la semana (window function)
WITH stats AS (
  SELECT
    DAYOFWEEK(pickup_date) AS day_of_week,
    pickup_hour,
    SUM(fare_amount) AS revenue
  FROM workspace.taxi_demo.trips_silver
  GROUP BY DAYOFWEEK(pickup_date), pickup_hour
)
SELECT 
  day_of_week,
  pickup_hour,
  revenue,
  DENSE_RANK() OVER (PARTITION BY day_of_week ORDER BY revenue DESC) AS rank_hour -- si 2 empatan, el que va siguiente es estrictamente el número que continua (ej rank 1, 2, 2, luego el siguiente va 3)
FROM stats
QUALIFY rank_hour <= 3
ORDER BY day_of_week, rank_hour;

-- COMMAND ----------

--Query 3: Crecimiento día contra día (LAG)
SELECT
  pickup_date,
  total_revenue,
  LAG(total_revenue) OVER (ORDER BY pickup_date) AS revenue_dia_anterior,
  ROUND( ((total_revenue / revenue_dia_anterior) -1) *100.0 , 2) as pct_change
FROM workspace.taxi_demo.daily_metrics
ORDER BY pickup_date;

-- COMMAND ----------

--Query 4: 20 Rutas más comunes (origen → destino)
SELECT
  pickup_zip,
  dropoff_zip,
  COUNT(*) AS trips,
  ROUND(AVG(fare_amount), 2) AS avg_fare,
  ROUND(AVG(trip_distance), 2) AS avg_distance
FROM workspace.taxi_demo.trips_silver
WHERE   pickup_zip IS NOT NULL 
        AND dropoff_zip IS NOT NULL
GROUP BY pickup_zip, dropoff_zip
HAVING COUNT(*) >= 5 -- que haya >= 5 viajes entre origen-destino codigo postal
ORDER BY trips DESC
LIMIT 20;