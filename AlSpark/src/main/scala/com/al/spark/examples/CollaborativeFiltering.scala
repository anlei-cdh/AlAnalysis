package com.al.spark.examples

import com.al.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.recommendation.ALS

/**
  * Created by AnLei on 2017/11/14.
  *
  * CollaborativeFiltering
  */
object CollaborativeFiltering {

  val count = 5
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

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

    val userRecs = model.recommendForAllUsers(count)
    val movieRecs = model.recommendForAllItems(count)

    userRecs.orderBy("userId").show(false)
    movieRecs.orderBy("movieId").show(false)

    spark.stop()
  }

}
