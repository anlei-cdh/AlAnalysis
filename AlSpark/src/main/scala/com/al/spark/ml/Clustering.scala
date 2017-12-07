package com.al.spark.ml

import java.sql.{Connection, PreparedStatement}

import com.al.basic.BasicDao
import com.al.config.Config
import com.al.db.DBHelper
import com.al.entity.DataResult
import com.al.util.MLUtil
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.functions.sum
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
    val clusteringResult = processClustering(clusteringDataFrame, spark)
    val prediction = getExplodeGroupData(clusteringResult)
    saveClusteringDB(prediction, spark)

    spark.stop()
  }

  def saveClusteringDB(prediction: DataFrame, spark: SparkSession): Unit = {
    prediction.repartition(Config.partition).foreachPartition(records => {
      if (!records.isEmpty) {
        val conn: Connection = DBHelper.getConnectionAtFalse()
        val sql: String = "INSERT INTO ml_clustering_data(clusteringid,channelid,pv) VALUES (#{prediction},#{dimeid},#{pv}) on duplicate key update pv = values(pv)"
        val pstmt: PreparedStatement = conn.prepareStatement(BasicDao.getRealSql(sql))
        var count: Int = 0

        records.foreach {
          record => {
            val dataResult = new DataResult
            dataResult.prediction = record.getAs[Int]("prediction")
            dataResult.dimeid = record.getAs[Int]("channelid")
            dataResult.pv = record.getAs[Long]("count").toInt

            count += 1
            DBHelper.setPreparedSqlexecuteBatch(conn, pstmt, sql, count, dataResult)
          }
        }

        DBHelper.commitClose(conn, pstmt)
      }
    })
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

  def processClustering(clusteringDataFrame: DataFrame, spark: SparkSession): DataFrame = {
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
    val clusteringResult = model.transform(clustering)

    return clusteringResult
  }

  def getExplodeGroupData(clusteringResult: DataFrame): DataFrame = {
    /**
      * [8.0, 8.0, 1.0, 8.0] ->
      * 8.0
      * 8.0
      * 1.0
      * 8.0
      */
    val explode = clusteringResult.selectExpr("prediction", "explode(textlist) AS text")

    /**
      * 聚类结果和频道标签分组
      */
    val groupPredictionCount = explode.groupBy("prediction","text").count().selectExpr("prediction","cast(text as int) as channelid","count")

    /**
      * 只按聚类结果分组 相当于标签数据加和
      * 增加频道标签列为了union
      */
    val groupCount = groupPredictionCount.groupBy("prediction").agg(sum("count") as "count")
    val groupCountAdd = groupCount.selectExpr("prediction", "(prediction * 0) as channelid", "count")

    return groupPredictionCount.union(groupCountAdd)
  }
}
