package com.al.util

import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.DataFrame

object MLUtil {

  def idfFeatures(sentenceData: DataFrame): DataFrame = {
    /**
      * 分词
      */
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    /**
      * 向量化
      */
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)

    /**
      * TF-IDF
      */
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)

    rescaledData
  }

}
