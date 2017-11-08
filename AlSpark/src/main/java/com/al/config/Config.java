package com.al.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Config {
	
	/**
	 * mysql数据库配置
	 */
	public static String driver_class;
	public static String db_url;
	public static String username;
	public static String password;
	public static int checkout_timeout;

	/**
	 * 静态参数配置
	 */
	public static Boolean is_local = false;
	public static String day;
	public static String input_path;

	/**
	 * Kafka参数
	 */
	public static String topic;
	public static String zkHosts;
	public static String brokerList;

	static {
		PropertiesConfiguration config = null;
		try {
			config = new PropertiesConfiguration("config/aura.properties");
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		loadConfig(config);
	}

	private static void loadConfig(PropertiesConfiguration config) {
		if(config.containsKey("is_local")) {
			is_local = config.getBoolean("is_local");
		}
		if(config.containsKey("day")) {
			day = config.getString("day");
			input_path = "logs/aura" + day.replace("-", "") + ".log";
		}

		if(config.containsKey("driver_class")) {
			driver_class = config.getString("driver_class");
		}
		if(config.containsKey("db_url")) {
			db_url = config.getString("db_url");
		}
		if(config.containsKey("username")) {
			username = config.getString("username");
		}
		if(config.containsKey("password")) {
			password = config.getString("password");
		}
		if(config.containsKey("checkout_timeout")) {
			checkout_timeout = config.getInt("checkout_timeout");
		}

		if(config.containsKey("topic")) {
			topic = config.getString("topic");
		}
		if(config.containsKey("zkHosts")) {
			zkHosts = config.getString("zkHosts");
		}
		if(config.containsKey("brokerList")) {
			brokerList = config.getString("brokerList");
		}
	}
	
	public static void main(String[] args) {
		System.out.println(Config.is_local);
		System.out.println(Config.day);
		System.out.println(Config.input_path);

		System.out.println(Config.driver_class);
		System.out.println(Config.db_url);
		System.out.println(Config.username);
		System.out.println(Config.password);
		System.out.println(Config.checkout_timeout);

		System.out.println(Config.topic);
		System.out.println(Config.zkHosts);
		System.out.println(Config.brokerList);
	}
}