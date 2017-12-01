package com.al.spark.ml

import com.al.util.{MLUtil, TrainingUtil}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.sql.SparkSession

object DecisionTree {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val path = "model/dt"
    val numFeatures = 10000

    /**
      * 训练集
      */
    val trainingDataFrame = spark.createDataFrame(TrainingUtil.trainingData).toDF("id", "text", "label")
    /**
      * 分词,向量化
      */
    val training = MLUtil.hashingFeatures(trainingDataFrame, numFeatures).select("label", "features")

    /**
      * 决策树模型
      */
    val dt = new DecisionTreeClassifier()
    /**
      * 保存模型
      */
    dt.fit(training).write.overwrite().save(path)

    /**
      * 测试集
      */
    val testDataFrame = spark.createDataFrame(TrainingUtil.testData).toDF("id", "text")
    /**
      * 分词,向量化
      */
    val test = MLUtil.hashingFeatures(testDataFrame, numFeatures).select("features")
    /**
      * 读取模型
      */
    val model = DecisionTreeClassificationModel.load(path)
    /**
      * 分类结果
      */
    val result = model.transform(test)

    result.show(false)

    spark.stop()
  }

}
