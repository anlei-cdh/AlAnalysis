package com.al.spark.ml

import com.al.config.Config
import com.al.util.{MLUtil, WordSplitUtil}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  val lr_path = "model/lr"
  val training_path = "training/gender.txt"
  val numFeatures = 10000

  val id = "id"
  val text = "text"
  val label = "label"
  val features = "features"

  case class Lr(uuid:String,ip:String,title: String,var text: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    // saveLogisticRegressionModel(spark)
    // testLogisticRegression(spark)

    val dataframe = spark.read.json(Config.input_path)
    val selectdf = dataframe.selectExpr("uuid", "ip", "title", "title AS text")

    import spark.implicits._
    val dataset = selectdf.as[Lr]
    val wordsplit = dataset.map{lr =>
      lr.text = WordSplitUtil.getWordSplit(lr.title)
      lr
    }.filter(lr => lr.text != null).select("uuid","ip",text)

    val idf = MLUtil.idfFeatures(wordsplit, numFeatures).select("uuid","ip",features)

    val lrModel = LogisticRegressionModel.load(lr_path)
    val result = lrModel.transform(idf).select("uuid","ip","prediction")

    result.show()

    spark.stop()
  }

  def saveLogisticRegressionModel(spark: SparkSession): Unit = {
    val trainingData = WordSplitUtil.getTrainingSplitList(training_path)

    val trainingDataFrame = spark.createDataFrame(trainingData).toDF(label, text)
    val training = MLUtil.idfFeatures(trainingDataFrame, numFeatures).select(label, features)

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    lr.fit(training).write.overwrite().save(lr_path)
  }

  def testLogisticRegression(spark: SparkSession): Unit = {
    val testData = Seq(
      (1, "特朗普 中国 挑衅"),
      (2, "市场经济国 中国 承认 地位"),
      (3, "恒大 中超 亚洲 重返"),
      (4, "辣妈 章泽天 诺奖 得主")
    )

    val testDataFrame = spark.createDataFrame(testData).toDF(id, text)
    val test = MLUtil.idfFeatures(testDataFrame, numFeatures).select(features)

    val lrModel = LogisticRegressionModel.load(lr_path)
    val result = lrModel.transform(test)

    result.show(false)
  }

}
