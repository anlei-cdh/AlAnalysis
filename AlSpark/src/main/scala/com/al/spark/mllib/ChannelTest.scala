package com.al.spark.mllib

import com.al.util.{FileUtil, SparkUtil}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.feature.HashingTF

/**
  * 贝叶斯分类算法
  * 测试类
  */
object ChannelTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext(this.getClass)
    val model = NaiveBayesModel.load(sc, "model/bayes_model")

    val tf = new HashingTF(numFeatures = 10000)
    val predictionAndLabel = model.predict(tf.transform(FileUtil.getTrainingString("特朗普三天内两度挑衅中国")))
    println(predictionAndLabel)
  }
}
