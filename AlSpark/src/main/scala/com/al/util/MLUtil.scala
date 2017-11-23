package com.al.util

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame

object MLUtil {
  /**
    * 提取特征 - 分词 + 向量化
    * @param dataFrame
    * @param numFeatures
    * @return
    */
  def hashingFeatures(dataFrame: DataFrame, numFeatures: Int): DataFrame = {
    hashingFeatures(dataFrame, numFeatures, "features")
  }

  /**
    * 提取特征公共方法 - 分词 + 向量化
    * @param dataFrame
    * @param numFeatures
    * @param outputCol
    * @return
    */
  def hashingFeatures(dataFrame: DataFrame, numFeatures: Int,outputCol: String): DataFrame = {
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val wordsData = tokenizer.transform(dataFrame)

    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(outputCol).setNumFeatures(numFeatures)
    val featurizedData = hashingTF.transform(wordsData)

    featurizedData
  }

  /**
    * 提取特征 - 分词 + 向量化 + IDF算法
    * @param dataFrame
    * @param numFeatures
    * @return
    */
  def idfFeatures(dataFrame: DataFrame, numFeatures: Int): DataFrame = {
    /**
      * 分词 + 向量化
      */
    val featurizedData = hashingFeatures(dataFrame, numFeatures, "rawFeatures")

    /**
      * TF-IDF
      */
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    // rescaledData.show(false)

    rescaledData
  }
}
