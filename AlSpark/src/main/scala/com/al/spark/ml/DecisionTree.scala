package com.al.spark.ml

import com.al.config.Config
import com.al.util.{MLUtil, TrainingUtil, WordSplitUtil}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.SparkSession

object DecisionTree {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

//    val path = "model/dt"
//    val numFeatures = 10000
//
//    /**
//      * 训练集
//      */
//    val trainingDataFrame = spark.createDataFrame(TrainingUtil.trainingData).toDF("id", "text", "label")
//    /**
//      * 分词,向量化
//      */
//    val training = MLUtil.hashingFeatures(trainingDataFrame, numFeatures).select("label", "features")
//
//    /**
//      * 决策树模型
//      */
//    val dt = new DecisionTreeClassifier()
//    /**
//      * 保存模型
//      */
//    dt.fit(training).write.overwrite().save(path)
//
//    /**
//      * 测试集
//      */
//    val testDataFrame = spark.createDataFrame(TrainingUtil.testData).toDF("id", "text")
//    /**
//      * 分词,向量化
//      */
//    val test = MLUtil.hashingFeatures(testDataFrame, numFeatures).select("features")
//    /**
//      * 读取模型
//      */
//    val model = DecisionTreeClassificationModel.load(path)
//    /**
//      * 分类结果
//      */
//    val result = model.transform(test)
//
//    result.show(false)

    saveDecisionTreeModel(spark)
    testDecisionTree(spark)

    spark.stop()
  }

  def saveDecisionTreeModel(spark: SparkSession): Unit = {
    val trainingData = WordSplitUtil.getTrainingSplitList(Config.training_channel_path)

    val trainingDataFrame = spark.createDataFrame(trainingData).toDF(Config.label, Config.text)
    val training = MLUtil.hashingFeatures(trainingDataFrame, Config.numFeatures).select(Config.label, Config.features)

    val dt = new DecisionTreeClassifier()
      .setImpurity("entropy")

    dt.fit(training).write.overwrite().save(Config.dt_path)
  }

  def testDecisionTree(spark: SparkSession): Unit = {
    val testDataFrame = spark.createDataFrame(TrainingUtil.testDtData).toDF(Config.id, Config.text)
    val test = MLUtil.hashingFeatures(testDataFrame, Config.numFeatures).select(Config.features)

    val model = DecisionTreeClassificationModel.load(Config.dt_path)
    val result = model.transform(test)

    result.show(false)
  }

}
