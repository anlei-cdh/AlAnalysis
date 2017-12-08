package com.al.spark.ml

import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

object CollaborativeFiltering {

  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName(s"${this.getClass.getSimpleName}").getOrCreate()

    import spark.implicits._
    val ratings = spark.read.textFile("logs/sample_movielens_ratings.txt")
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

    val userRecs = model.recommendForAllUsers(10)
    val movieRecs = model.recommendForAllItems(10)

    userRecs.show(false)
    movieRecs.show(false)

    spark.stop()
  }

}
