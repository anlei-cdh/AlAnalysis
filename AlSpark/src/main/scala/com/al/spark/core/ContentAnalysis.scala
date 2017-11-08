package com.al.spark.core

import java.sql.Connection

import com.al.config.Config
import com.al.dao.ContentDao
import com.al.db.DBHelper
import com.al.entity.Content
import com.al.util.{SparkUtil, StringUtil}

/**
  * Created by An on 2016/11/25.
  * 稿件热度计算
  */
object ContentAnalysis {

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
      * 计算稿件热度(map,reduce)
      */
    val map = filter.filter{log => {log.getPagetype() == '1' && log.getClearTitle() != null && log.getClearTitle().length() > 5}}.map(
      log => {
        val content: Content = new Content(pv = 1, uv = 1)
        content.contentId = log.ContentId
        content.uvs += log.Uuid
        content.url = log.Url
        content.title = log.getClearTitle()
        (content.contentId, content)
      }
    ).cache()
    /**
      * 计算稿件的pv,uv
      */
    val reduce = map.reduceByKey((m, n) => {
      m.pv += n.pv
      m.uvs ++= n.uvs
      m.uv = m.uvs.size
      (m)
    })

    val list: List[Content] = reduce.values.collect().toList
    list.foreach(item => {
      item.day = Config.day
      item.url = StringUtil.limitString(item.url, 500, "utf8")
      item.title = StringUtil.limitString(item.title, 500, "utf8")
    })
    sc.stop()
    /**
      * 写入数据库
      */
    val conn: Connection = DBHelper.getConnection()
    try {
      ContentDao.saveContentData(list, conn)
      ContentDao.saveContentDetail(list, conn)
    } finally {
      DBHelper.close(conn)
    }
  }

  def main(args: Array[String]): Unit = {
    runAnalysis()
  }
}
