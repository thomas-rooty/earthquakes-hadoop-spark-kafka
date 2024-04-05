from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Analyse Sismique Horaires") \
    .getOrCreate()

# Load hdfs data
df_sismique = spark.read.csv("hdfs://namenode:9000/sismique/data/dataset_sismique.csv", inferSchema=True, header=True)
df_villes = spark.read.csv("hdfs://namenode:9000/sismique/data/dataset_sismique_villes.csv", inferSchema=True, header=True)

df_sismique = df_sismique.withColumn("date", to_timestamp("date", 'yyyy-MM-dd HH:mm:ss'))
df_villes = df_villes.withColumn("date", to_timestamp("date", 'yyyy-MM-dd HH:mm:ss'))

# Aggregate by hour
df_sismique_hourly = df_sismique.groupBy(window("date", "1 hour").alias("hour")).agg(
    avg("magnitude").alias("magnitude_moyenne"),
    avg("tension entre plaque").alias("tension_moyenne")
)

# Aggregate by hour and city
df_villes_hourly = df_villes.groupBy("ville", window("date", "1 hour").alias("hour")).agg(
    avg("magnitude").alias("magnitude_moyenne"),
    avg("tension entre plaque").alias("tension_moyenne")
)

# Results
print("Sismique Horaires")
df_sismique_hourly.select("hour", "magnitude_moyenne", "tension_moyenne").show(truncate=False)

print("Sismique Villes Horaires")
df_villes_hourly.select("ville", "hour", "magnitude_moyenne", "tension_moyenne").show(truncate=False)

spark.stop()
