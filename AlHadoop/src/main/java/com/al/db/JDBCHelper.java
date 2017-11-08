package com.al.db;

import com.al.config.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JDBCHelper {
	
	public static Connection getConnection(){
		Connection conn = null;
		try {
			Class.forName(Config.driver_class);
			String url = Config.db_url;
			String username = Config.username;
			String password = Config.password;
			url += "&user=" + username + "&password=" + password;
			System.out.println(url);
			conn = DriverManager.getConnection(url);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void main(String[] args) {
		JDBCHelper.getConnection();
	}
}
