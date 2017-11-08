package com.al.db

import java.sql.{Connection, DriverManager}

import com.al.config.Config

object JDBCHelper {
  
  def getConnection(): Connection = {
    Class.forName(Config.driver_class)
    return DriverManager.getConnection(Config.db_url, Config.username, Config.password)
	}
  
  def main(args: Array[String]): Unit = {
    JDBCHelper.getConnection()
  }
}