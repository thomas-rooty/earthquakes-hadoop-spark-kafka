from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler


def entrainer_modele(data_path, model_path):
    spark = SparkSession.builder.appName("Entrainement Modele Sismique").getOrCreate()

    df = spark.read.parquet(data_path)

    # Assurez-vous que les données sont filtrées ou sélectionnées selon les besoins avant l'entraînement
    df = df.filter(df["secousse"] == True)

    (trainingData, testData) = df.randomSplit([0.7, 0.3])

    lr = LinearRegression(featuresCol="features", labelCol="magnitude")
    lrModel = lr.fit(trainingData)

    lrModel.write().overwrite().save(model_path)

    spark.stop()


if __name__ == "__main__":
    data_path = "hdfs://namenode:9000/sismique/data/prepared_data"
    model_path = "hdfs://namenode:9000/sismique/models/regression_model"
    entrainer_modele(data_path, model_path)
