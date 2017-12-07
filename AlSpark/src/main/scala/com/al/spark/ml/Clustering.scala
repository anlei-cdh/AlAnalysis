package com.al.spark.ml

import com.al.config.Config
import com.al.util.MLUtil
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}

object Clustering {

  case class Users(label: String, textlist: List[String], var text: String)

  val k = 5
  val numFeatures = 50

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    if(Config.is_local) {
      builder.master("local")
    }
    val spark = builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()

    val clusteringDataFrame = getClusteringDataFrame(spark)
    processClustering(clusteringDataFrame, spark)

    spark.stop()
  }

  def getClusteringDataFrame(spark: SparkSession): DataFrame = {
    DecisionTree.saveDecisionTreeModel(spark)
    val predictionDataFrame = DecisionTree.processDecisionTree(spark).filter("uuid is not null")

    import org.apache.spark.sql.functions._
    val df = predictionDataFrame.groupBy("uuid").agg(collect_list("prediction") as "prediction").selectExpr("uuid AS label", "uuid AS text", "prediction AS textlist")

    import  spark.implicits._
    val ds = df.as[Users]

    val clusteringDataFrame = ds.map(user => {
      user.text = user.textlist.mkString(" ")
      user
    }).toDF()

    return clusteringDataFrame
  }

  def processClustering(clusteringDataFrame: DataFrame, spark: SparkSession): Unit = {
    /**
      * 分词,向量化
      */
    val clustering = MLUtil.hashingFeatures(clusteringDataFrame, numFeatures).select("label", "features", "text", "textlist")

    /**
      * K-means模型
      */
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val model = kmeans.fit(clustering)

    /**
      * 聚类中心
      */
    // model.clusterCenters.foreach(println)
    /**
      * 聚类结果
      */
    model.transform(clustering).show(100)
  }
}
