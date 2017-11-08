package com.al.spark.mllib

import com.al.util.SparkUtil
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.feature.HashingTF

/**
  * 支持向量机算法
  * 测试类
  */
object GenderTest {
  def main(args: Array[String]): Unit = {
    val sc = SparkUtil.getSparkContext(this.getClass)
    val model = SVMModel.load(sc, "model/svm_model")

    val tf = new HashingTF(numFeatures = 10000)
    val predictionAndLabel = model.predict(tf.transform(Array("特朗普","中国","挑衅")))
    println(predictionAndLabel.toInt)
  }
}
