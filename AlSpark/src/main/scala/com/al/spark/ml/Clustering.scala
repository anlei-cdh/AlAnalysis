package com.al.spark.ml

import com.al.util.{MLUtil, TrainingUtil}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.SparkSession

object Clustering {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val numFeatures = 20
    /**
      * 训练集
      */
    val clusteringDataFrame = spark.createDataFrame(TrainingUtil.clusteringData).toDF("label", "text")
    /**
      * 分词,向量化
      */
    val clustering = MLUtil.hashingFeatures(clusteringDataFrame, numFeatures).select("label", "features")

    /**
      * K-means模型
      */
    val kmeans = new KMeans().setK(3).setSeed(1L)
    val model = kmeans.fit(clustering)

    /**
      * 聚类中心
      */
    model.clusterCenters.foreach(println)
    /**
      * 聚类结果
      */
    model.transform(clustering).show(false)

    spark.stop()
  }
}
