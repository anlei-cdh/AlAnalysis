package com.al.spark.core

import com.al.config.Config
import com.al.dao.DimensionDao
import com.al.entity.Dimension
import com.al.util.SparkUtil
import org.apache.spark.broadcast.Broadcast

/**
  * Created by An on 2016/11/25.
  * 搜索引擎计算
  */
object SearchAnalysis {

  def runAnalysis(): Unit = {
    /**
      * 获得SparkContext
      */
    val sc = SparkUtil.getSparkContext(this.getClass)
    /**
      * 读取日志
      */
    val lines = sc.textFile(Config.input_path)
    /**
      * 解析日志并过滤其中的错误内容
      */
    val filter = SparkUtil.getFilterLog(lines).cache()
    /**
      * 搜索引擎映射
      */
    val searchMap: Map[String, Dimension] = DimensionDao.getSearchEngineMap()
    /**
      * 广播变量
      */
    val searchBc: Broadcast[Map[String, Dimension]] = sc.broadcast(searchMap)

    /**
      * 计算搜索引擎(map,reduce)
      */
    val map = filter.map(
      log => {
        val dimension: Dimension = new Dimension(pv = 1,uv = 1,ip = 1)
        val search = log.SearchEngine
        dimension.uvs += log.Uuid
        dimension.ips += log.Ip
        if(search != null) {
          if(searchBc.value.contains(search)) {
            dimension.dimeId = searchBc.value.get(search).get.dimeId
          }
        }
        (dimension.dimeId, dimension)
      }
    ).filter(_._1 != 0).cache()
    /**
      * 计算维度pv,uv,ip的通用ReduceByKey
      */
    val reduce = SparkUtil.getReduceByKey(map)

    val list: List[Dimension] = reduce.values.collect().toList
    list.foreach(item => {
      item.day = Config.day
    })
    sc.stop()
    /**
      * 写入数据库
      */
    DimensionDao.saveDimensionList(list)
  }

  def main(args: Array[String]): Unit = {
    runAnalysis()
  }
}
