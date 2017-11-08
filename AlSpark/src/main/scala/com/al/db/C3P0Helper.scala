package com.al.db

import java.sql.Connection

import com.al.config.Config
import com.mchange.v2.c3p0.ComboPooledDataSource

/**
  * Created by An on 2016/11/29.
  */
object C3P0Helper {
  /**
    * 获得c3p0连接
    * @return
    */
  def getConnection(): Connection = {
      Class.forName(Config.driver_class);
      val cpds = initCPDS(Config.db_url)
      return cpds.getConnection
  }

  /**
    * 初始化c3p0数据源
    * @param url
    */
  private def initCPDS(url: String): ComboPooledDataSource = {
    // 批量提交
    var jdbcUrl = ""
    if (url.indexOf("?") < 0)
      jdbcUrl = url + "?rewriteBatchedStatements=true"
    else
      jdbcUrl = url + "&rewriteBatchedStatements=true"
    val thecpds: ComboPooledDataSource = new ComboPooledDataSource
    thecpds.setJdbcUrl(jdbcUrl)
    thecpds.setUser(Config.username)
    thecpds.setPassword(Config.password)
    thecpds.setCheckoutTimeout(Config.checkout_timeout)
    // the settings below are optional -- c3p0 can work with defaults
    thecpds.setMinPoolSize(1)
    thecpds.setInitialPoolSize(1)
    thecpds.setAcquireIncrement(1)
    thecpds.setMaxPoolSize(4)
    thecpds.setMaxIdleTime(1800)
    thecpds.setMaxStatements(0) // disable Statements cache to avoid deadlock
    thecpds.setPreferredTestQuery("select 1")
    thecpds.setTestConnectionOnCheckout(true)
    return thecpds
  }

  @throws[Exception]
  def main(args: Array[String]) {
    C3P0Helper.getConnection
  }
}
