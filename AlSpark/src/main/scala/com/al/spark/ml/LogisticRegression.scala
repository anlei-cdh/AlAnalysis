package com.al.spark.ml

import com.al.util.{MLUtil, WordSplitUtil}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/lr"
    val trainingData = WordSplitUtil.getTrainingSplitList("training/gender.txt")

    val trainingDataFrame = spark.createDataFrame(trainingData).toDF("label", "sentence")
    val training = MLUtil.idfFeatures(trainingDataFrame).select("label", "features")

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    lr.fit(training).save(path)

    val testData = spark.createDataFrame(Seq(
      (0.0, "特朗普 中国 挑衅"),
      (0.0, "市场经济国 中国 承认 地位"),
      (0.0, "恒大 中超 亚洲 重返"),
      (0.0, "辣妈 章泽天 诺奖 得主")
    )).toDF("label", "sentence")
    val test = MLUtil.idfFeatures(testData).select("features")

    val lrModel = LogisticRegressionModel.load(path)
    val result = lrModel.transform(test)
    result.show(false)

    spark.stop()

  }

}
