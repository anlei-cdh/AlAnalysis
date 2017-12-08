package com.al.config

import org.apache.commons.configuration.PropertiesConfiguration

object Config {

  /**
    * 静态参数配置
    */
  var is_local = false
  var base_path: String = null
  var day: String = null
  var input_path: String = null

  /**
    * mysql数据库配置
    */
  var driver_class: String = null
  var db_url: String = null
  var username: String = null
  var password: String = null
  var checkout_timeout = 0

  /**
    * Kafka参数
    */
  var topic: String = null
  var zkHosts: String = null
  var brokerList: String = null

  /**
    * Spark模块参数配置
    */
  val partition = 1
  val recommendcount = 5
  val lr_path = "model/lr"
  val dt_path = "model/dt"
  val training_gender_path = "training/gender.txt"
  val training_channel_path = "training/channel.txt"
  val numFeatures = 10000
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

    if (config.containsKey("topic")) topic = config.getString("topic")
    if (config.containsKey("zkHosts")) zkHosts = config.getString("zkHosts")
    if (config.containsKey("brokerList")) brokerList = config.getString("brokerList")

    if (config.containsKey("driver_class")) driver_class = config.getString("driver_class")
    if (config.containsKey("db_url")) db_url = config.getString("db_url")
    if (config.containsKey("username")) username = config.getString("username")
    if (config.containsKey("password")) password = config.getString("password")
    if (config.containsKey("checkout_timeout")) checkout_timeout = config.getInt("checkout_timeout")
  }

  def main(args: Array[String]): Unit = {
    println("===Parameter===")
    println(Config.is_local)
    println(Config.day)
    println(Config.base_path)
    println(Config.input_path)
    println("===MySql===")
    println(Config.driver_class)
    println(Config.db_url)
    println(Config.username)
    println(Config.password)
    println(Config.checkout_timeout)
    println("===Kafka===")
    println(Config.topic)
    println(Config.zkHosts)
    println(Config.brokerList)
  }
}
