package com.al.db;

import com.al.basic.BasicDao;
import com.al.config.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveHelper {
	
	public static Connection getConnection(){
		Connection conn = null;
		try {
			Class.forName(Config.hive_driver_class);
			String url = Config.hive_db_url;
			String username = Config.hive_username;
			String password = Config.hive_password;
			conn = DriverManager.getConnection(url,username,password);
			if(Config.is_local) { // 开启本地MR模式
				System.out.println("local hadoop");
				setLocalMr(conn);
			}
			addJar(conn);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * 执行多条SQL操作
	 */
	public static void executeSql(Connection conn,String ... sqls) {
		try {
			for(String sql : sqls) {
				BasicDao.executeSql(sql, conn);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 设置为本地MR
	 * @param conn
	 */
	public static void setLocalMr(Connection conn) {
		executeSql(conn,"SET mapred.job.tracker=local",
						"SET hive.exec.mode.local.auto=true",
						"SET hive.exec.mode.local.auto.input.files.max=4000",
						"SET hive.exec.mode.local.auto.inputbytes.max=9134217728");
	}

	/**
	 * 添加JsonSerDe
	 * @param conn
	 */
	public static void addJar(Connection conn) {
		executeSql(conn,"ADD JAR /jar/json-serde-1.3-jar-with-dependencies.jar");
	}
	
	public static void main(String[] args) {
		HiveHelper.getConnection();
	}
}
