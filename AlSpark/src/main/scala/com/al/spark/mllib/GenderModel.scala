package com.al.spark.mllib

import com.al.entity.Training
import com.al.util.{FileUtil, SparkUtil}
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable.ListBuffer

/**
  * 支持向量机算法
  * 生成模型
  */
object GenderModel {

  def main(args: Array[String]) {
    val sc = SparkUtil.getSparkContext(this.getClass)

    val list: ListBuffer[Training] = FileUtil.getTrainingList("training/gender.txt")
    val arr = FileUtil.getTrainingArrayBuffer(list)

    val data = sc.parallelize(arr)
    val tf = new HashingTF(numFeatures = 10000)
    val parsedData = data.map{ line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, tf.transform(parts(1).split(" ")))
    }.cache()
    
    val model = SVMWithSGD.train(parsedData, 100)

    // 保存模型
    model.save(sc, "model/svm_model")
  }
}
