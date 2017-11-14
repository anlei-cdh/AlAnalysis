package com.al.util

import com.al.spark.core._
import com.al.spark.mllib.{ChannelAnalysis, GenderAnalysis}

/**
  * Created by An on 2016/11/30.
  */
object AllAnalysis {

  def main(args: Array[String]): Unit = {
    FlowAnalysis.runAnalysis()
    SearchAnalysis.runAnalysis()
    ProvinceAnalysis.runAnalysis()
    CountryAnalysis.runAnalysis()
    ContentAnalysis.runAnalysis()

    GenderAnalysis.runAnalysis()
    ChannelAnalysis.runAnalysis()
  }

}