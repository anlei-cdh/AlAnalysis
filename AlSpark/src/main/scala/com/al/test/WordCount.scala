package com.al.test

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName()).setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("logs/wordcount.log")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect
    result.foreach(println(_))
  }
}