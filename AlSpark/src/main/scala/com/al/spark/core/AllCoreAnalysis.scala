package com.al.spark.core

/**
  * Created by An on 2016/11/30.
  */
object AllCoreAnalysis {
  def main(args: Array[String]): Unit = {
    FlowAnalysis.runAnalysis()
    SearchAnalysis.runAnalysis()
    ProvinceAnalysis.runAnalysis()
    CountryAnalysis.runAnalysis()
    ContentAnalysis.runAnalysis()
  }
}