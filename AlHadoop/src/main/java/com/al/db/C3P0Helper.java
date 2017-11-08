package com.al.db;

import com.al.config.Config;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class C3P0Helper {
	private static ComboPooledDataSource cpds;
	
	static {
		try {
			Class.forName(Config.driver_class);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		cpds = initCPDS(Config.db_url);
	}
	
	// 数据源
	public static Connection getConnection() {
		try {
			return cpds.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 初始化c3p0数据源
	 * @param url
	 */
	private static ComboPooledDataSource initCPDS(String url) {
		// 批量提交
		url += url.indexOf("?") < 0 ? "?rewriteBatchedStatements=true" : "&rewriteBatchedStatements=true";
		ComboPooledDataSource thecpds = new ComboPooledDataSource();
		thecpds.setJdbcUrl(url); 
		thecpds.setUser(Config.username); 
		thecpds.setPassword(Config.password); 
		thecpds.setCheckoutTimeout(Config.checkout_timeout);
		// the settings below are optional -- c3p0 can work with defaults 
		thecpds.setMinPoolSize(1); 
		thecpds.setInitialPoolSize(1);
		thecpds.setAcquireIncrement(1); 
		thecpds.setMaxPoolSize(4);
		thecpds.setMaxIdleTime(1800);
		thecpds.setMaxStatements(0); // disable Statements cache to avoid deadlock
		thecpds.setPreferredTestQuery("select 1");
		thecpds.setTestConnectionOnCheckout(true);

		return thecpds;
	}
	
	public static void main(String[] args) throws Exception {
		C3P0Helper.getConnection();
	}

}
