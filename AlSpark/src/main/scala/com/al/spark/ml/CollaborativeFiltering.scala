package com.al.spark.ml

import java.sql.{Connection, PreparedStatement}

import com.al.basic.BasicDao
import com.al.config.Config
import com.al.db.DBHelper
import com.al.entity.DataResult
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{Row, SparkSession}

object CollaborativeFiltering {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
    if(Config.is_local) {
      builder.master("local")
    }
    val spark = builder.appName(s"${this.getClass.getSimpleName}").getOrCreate()

    runAlsRecommend(spark)

    spark.stop()
  }

  def runAlsRecommend(spark: SparkSession): Unit = {
    import spark.implicits._
    val ratings = spark.read.textFile(Config.cf_data)
      .map(parseRating)
      .toDF()

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
    val model = als.fit(ratings)
    model.setColdStartStrategy("drop")

    val userRecs = model.recommendForAllUsers(Config.recommendcount)
    val userRecommend = userRecs.selectExpr("userId", "explode(recommendations) AS reco")

    userRecommend.repartition(Config.partition).foreachPartition(records => {
      if (!records.isEmpty) {
        val conn: Connection = DBHelper.getConnectionAtFalse()

        val sql: String = "INSERT INTO ml_cf_user_data(userid,itemid,source) VALUES (#{userid},#{itemid},#{source}) on duplicate key update source = values(source)"
        val pstmt: PreparedStatement = conn.prepareStatement(BasicDao.getRealSql(sql))
        var count: Int = 0

        records.foreach {
          record => {
            val row = record.getAs[Row]("reco")
            val userid = record.getAs[Int]("userId")
            val itemid = row.getInt(0)
            val source = row.getFloat(1)

            val dataResult = new DataResult
            dataResult.userid = userid
            dataResult.itemid = itemid
            dataResult.source = source

            count += 1
            DBHelper.setPreparedSqlexecuteBatch(conn, pstmt, sql, count, dataResult)
          }
        }

        DBHelper.commitClose(conn, pstmt)
      }
    })

    val movieRecs = model.recommendForAllItems(Config.recommendcount)
    val itemRecommend = movieRecs.selectExpr("movieId as itemId", "explode(recommendations) AS reco")

    itemRecommend.repartition(Config.partition).foreachPartition(records => {
      if (!records.isEmpty) {
        val conn: Connection = DBHelper.getConnectionAtFalse()

        val sql: String = "INSERT INTO ml_cf_item_data(userid,itemid,source) VALUES (#{userid},#{itemid},#{source}) on duplicate key update source = values(source)"
        val pstmt: PreparedStatement = conn.prepareStatement(BasicDao.getRealSql(sql))
        var count: Int = 0

        records.foreach {
          record => {
            val row = record.getAs[Row]("reco")
            val itemid = record.getAs[Int]("itemId")
            val userid = row.getInt(0)
            val source = row.getFloat(1)

            val dataResult = new DataResult
            dataResult.userid = userid
            dataResult.itemid = itemid
            dataResult.source = source

            count += 1
            DBHelper.setPreparedSqlexecuteBatch(conn, pstmt, sql, count, dataResult)
          }
        }

        DBHelper.commitClose(conn, pstmt)
      }
    })
  }

}
