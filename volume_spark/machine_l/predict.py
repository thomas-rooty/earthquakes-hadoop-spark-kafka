from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel


def faire_predictions(data_path, model_path):
    spark = SparkSession.builder.appName("Predictions Sismiques").getOrCreate()

    df = spark.read.parquet(data_path)
    lrModel = LinearRegressionModel.load(model_path)

    predictions = lrModel.transform(df)
    predictions.select("prediction", "magnitude", "features").show(5)

    spark.stop()


if __name__ == "__main__":
    data_path = "hdfs://namenode:9000/sismique/data/prepared_data"
    model_path = "hdfs://namenode:9000/sismique/models/regression_model"
    faire_predictions(data_path, model_path)
