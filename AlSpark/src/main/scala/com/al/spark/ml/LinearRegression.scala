package com.al.spark.ml

import com.al.config.Config
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession

object LinearRegression {

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    if(Config.is_local) {
      builder.master("local")
    }
    val spark = builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()

    /**
      * 房屋售价
      * ID，平米数，售价(万元)
      */
    val trainingData = Seq(
      (1, Vectors.dense(123), 490),
      (2, Vectors.dense(150), 630),
      (3, Vectors.dense(49), 120),
      (4, Vectors.dense(58), 165),
      (5, Vectors.dense(68), 215),
      (6, Vectors.dense(78), 265),
      (7, Vectors.dense(87), 310),
      (8, Vectors.dense(115), 450),
      (9, Vectors.dense(120), 475),
      (10, Vectors.dense(135), 550)
    )
    val trainingDataFrame = spark.createDataFrame(trainingData).toDF("id", "features", "label")
    trainingDataFrame.show(false)

    val line = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
    val lineModel = line.fit(trainingDataFrame)

    /**
      * 预测100平米房屋的售价(万元)
      */
    val testData = Seq(
      (1, Vectors.dense(100))
    )

    val testDataFrame = spark.createDataFrame(testData).toDF("id", "features")
    lineModel.transform(testDataFrame).show(false)

    spark.stop()
  }

}
