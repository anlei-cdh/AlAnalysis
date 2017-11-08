package com.al.util

import com.al.config.Config
import com.al.entity.{Dimension, Log}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by An on 2016/11/25.
  */
object SparkUtil {
  /**
    * 获得SparkConf
    * @param cls
    * @return
    */
  def getSparkConf(cls: Class[_]): SparkConf = {
    val conf = new SparkConf().setAppName(cls.getSimpleName())
    if(Config.is_local) {
      conf.setMaster("local[*]")
    }
    return conf
  }

  /**
    * 获得SparkContext
    * @param cls
    * @return
    */
  def getSparkContext(cls: Class[_]): SparkContext = {
    return new SparkContext(getSparkConf(cls))
  }

  /**
    * 解析日志并过滤其中的错误内容
    * @param lines
    * @return
    */
  def getFilterLog(lines: RDD[String]): RDD[Log] = {
    return lines.map(
      line => {
        val log: Log = JSON.parseObject(line, classOf[Log])
        if(log.isLegal()) {
          (log)
        } else {
          (null)
        }
      }
    ).filter(_ != null)
  }

  /**
    * 计算维度pv,uv,ip的通用ReduceByKey
    * 页面浏览量直接累加
    * 访问者和访问者IP数需要归并
    * @param map
    * @return
    */
  def getReduceByKey(map: RDD[(Int,Dimension)]): RDD[(Int,Dimension)] = {
    return map.reduceByKey((m, n) => {
      m.pv += n.pv
      m.uvs ++= n.uvs
      m.ips ++= n.ips
      m.uv = m.uvs.size
      m.ip = m.ips.size
      (m)
    })
  }
}
