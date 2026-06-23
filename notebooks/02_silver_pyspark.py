# Databricks notebook source
# Silver en PySpark: misma lógica que 02_silver.sql, con DataFrame API
# Objetivo: comparar Spark SQL vs PySpark DataFrame API y validar paridad
from pyspark.sql import functions as F

# COMMAND ----------

df_bronze = spark.table("workspace.taxi_demo.trips_bronze")

# COMMAND ----------

# Transformación Silver: limpieza de inválidos/outliers + columnas derivadas
df_silver = (
    df_bronze
    # filtros: mismos que el WHERE del SQL
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("fare_amount") < 500)
    .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
    # columnas derivadas
    .select(
        F.col("tpep_pickup_datetime").alias("pickup_ts"),
        F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        F.col("trip_distance"),
        F.col("fare_amount"),
        F.col("pickup_zip"),
        F.col("dropoff_zip"),
        ((F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)
            .alias("trip_duration_min"),
    )
)

# COMMAND ----------

# Escribir como tabla Delta (target separado para no pisar la versión SQL)
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.taxi_demo.trips_silver_pyspark"))

# COMMAND ----------

# Validación de paridad: comparar PySpark vs SQL
sql_count = spark.table("workspace.taxi_demo.trips_silver").count()
py_count  = spark.table("workspace.taxi_demo.trips_silver_pyspark").count()

print(f"Filas SQL:     {sql_count}")
print(f"Filas PySpark: {py_count}")
print(f"Paridad de row count: {sql_count == py_count}")

# COMMAND ----------

# comparar agregaciones clave
from pyspark.sql import functions as F

agg_sql = spark.table("workspace.taxi_demo.trips_silver").agg(
    F.round(F.sum("fare_amount"), 2).alias("total_fare"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration")
).collect()[0]

agg_py = spark.table("workspace.taxi_demo.trips_silver_pyspark").agg(
    F.round(F.sum("fare_amount"), 2).alias("total_fare"),
    F.round(F.avg("trip_duration_min"), 2).alias("avg_duration")
).collect()[0]

print(f"SQL     -> total_fare: {agg_sql['total_fare']}, avg_duration: {agg_sql['avg_duration']}")
print(f"PySpark -> total_fare: {agg_py['total_fare']}, avg_duration: {agg_py['avg_duration']}")
print(f"Paridad total_fare: {agg_sql['total_fare'] == agg_py['total_fare']}")
print(f"Paridad avg_duration: {abs(float(agg_sql['avg_duration']) - float(agg_py['avg_duration'])) < 1e-6}")

# COMMAND ----------

# ============================================================
# APÉNDICE — Técnicas de PySpark (no forman parte de la capa Silver)
# Estas celdas no modifican trips_silver_pyspark ni la validación de arriba.
# Son ejemplos independientes para demostrar broadcast join y window
# functions sobre el mismo dataset. La paridad SQL/PySpark se valida
# únicamente sobre la transformación Silver de la sección anterior.
# ============================================================

# --- Broadcast join ---
# Tabla chica de referencia (zonas por zip de pickup) -> broadcasteo todos los nodos y evito shuffle
df_zones = spark.createDataFrame(
    [
        (10001, "Manhattan"),
        (10002, "Manhattan"),
        (11201, "Brooklyn"),
        (11101, "Queens"),
        (10451, "Bronx"),
    ],
    ["zip", "borough"]
)

df_enriched = df_silver.join(
    F.broadcast(df_zones),
    df_silver.pickup_zip == df_zones.zip,
    "left"
).drop("zip")

df_enriched.select("pickup_zip", "borough", "fare_amount").show(5)

# COMMAND ----------


# --- Window function ---
# Ranking de viajes por fare dentro de cada día (top fares por día)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("pickup_date").orderBy(F.col("fare_amount").desc())

df_ranked = df_silver.withColumn(
    "fare_rank_in_day",
    F.dense_rank().over(window_spec)
)

# Top 3 viajes más caros por día
(df_ranked
    .filter(F.col("fare_rank_in_day") <= 3)
    .select("pickup_date", "fare_amount", "trip_distance", "fare_rank_in_day")
    .orderBy("pickup_date", "fare_rank_in_day")
    .show(10))
