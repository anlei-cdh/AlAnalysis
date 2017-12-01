package com.al.config

import org.apache.commons.configuration.PropertiesConfiguration

object Config {
  /**
    * mysql数据库配置
    */
  var driver_class: String = null
  var db_url: String = null
  var username: String = null
  var password: String = null
  var checkout_timeout = 0

  /**
    * hive数据库配置
    */
  var hive_driver_class: String = null
  var hive_db_url: String = null
  var hive_username: String = null
  var hive_password: String = null

  /**
    * 静态参数配置
    */
  var is_local = false
  var base_path: String = null
  var day: String = null
  var input_path: String = null
  var output_path = "output/time"
  var newline = "\n"
  var backslash = "/"
  var default_ses_time = 1800
  var reduce_result_filename = "part-r-00000"

  /**
    * Kafka参数
    */
  var topic: String = null
  var zkHosts: String = null
  var brokerList: String = null

  /**
    * 机器学习列名
    */
  val id = "id"
  val text = "text"
  val label = "label"
  val features = "features"

  val config: PropertiesConfiguration = new PropertiesConfiguration("config/al.properties");
  loadConfig(config)

  private def loadConfig(config: PropertiesConfiguration): Unit = {
    if (config.containsKey("is_local")) is_local = config.getBoolean("is_local")
    if (config.containsKey("base_path") && config.containsKey("day")) {
      base_path = config.getString("base_path")
      day = config.getString("day")
      input_path = base_path + day.replace("-", "") + ".log"
    }
    if (config.containsKey("driver_class")) driver_class = config.getString("driver_class")
    if (config.containsKey("db_url")) db_url = config.getString("db_url")
    if (config.containsKey("username")) username = config.getString("username")
    if (config.containsKey("password")) password = config.getString("password")
    if (config.containsKey("checkout_timeout")) checkout_timeout = config.getInt("checkout_timeout")
    if (config.containsKey("hive_driver_class")) hive_driver_class = config.getString("hive_driver_class")
    if (config.containsKey("hive_db_url")) hive_db_url = config.getString("hive_db_url")
    if (config.containsKey("hive_username")) hive_username = config.getString("hive_username")
    if (config.containsKey("hive_password")) hive_password = config.getString("hive_password")
    if (config.containsKey("topic")) topic = config.getString("topic")
    if (config.containsKey("zkHosts")) zkHosts = config.getString("zkHosts")
    if (config.containsKey("brokerList")) brokerList = config.getString("brokerList")
  }

  def main(args: Array[String]): Unit = {
    println(Config.is_local)
    println(Config.base_path)
    println(Config.day)
    println(Config.input_path)
    println(Config.driver_class)
    println(Config.db_url)
    println(Config.username)
    println(Config.password)
    println(Config.checkout_timeout)
    println(Config.hive_driver_class)
    println(Config.hive_db_url)
    println(Config.hive_username)
    println(Config.hive_password)
    println(Config.topic)
    println(Config.zkHosts)
    println(Config.brokerList)
  }
}
