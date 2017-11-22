package com.al.spark.ml

import com.al.util.MLUtil
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/lr"

    val trainingData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val training = MLUtil.idfFeatures(trainingData).select("label", "features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    lr.fit(training).save(path)

    val testData = spark.createDataFrame(Seq(
      (0.0, "Hi I'd like spark"),
      (0.0, "I wish Java could use goland"),
      (0.0, "Linear regression models are neat"),
      (0.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    val test = MLUtil.idfFeatures(testData).select("features")

    val lrModel = LogisticRegressionModel.load(path)
    val result = lrModel.transform(test)
    result.show(false)

    spark.stop()

  }

}
