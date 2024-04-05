from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, minute
from pyspark.ml.feature import VectorAssembler


def preparer_donnees(input_path, output_path):
    spark = SparkSession.builder.appName("Preparation des Donnees Sismiques").getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df = df.withColumn("heure", hour(df["date"])).withColumn("minute", minute(df["date"]))

    vectorAssembler = VectorAssembler(inputCols=["heure", "minute", "tension entre plaque"], outputCol="features")
    df = vectorAssembler.transform(df)

    df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    input_path = "hdfs://namenode:9000/sismique/data/dataset_sismique.csv"
    output_path = "hdfs://namenode:9000/sismique/data/prepared_data"
    preparer_donnees(input_path, output_path)
