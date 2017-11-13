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
	 * hive数据库配置
	 */
	public static String hive_driver_class;
	public static String hive_db_url;
	public static String hive_username;
	public static String hive_password;

	/**
	 * 静态参数配置
	 */
	public static Boolean is_local = false;
	public static String day;
	public static String input_path;
	public static String output_path = "output/time";
	public static String newline = "\n";
	public static String backslash = "/";
	public static long default_ses_time = 1800;
	public static String reduce_result_filename = "part-r-00000";

	/**
	 * Kafka参数
	 */
	public static String topic;
	public static String zkHosts;
	public static String brokerList;

	static {
		PropertiesConfiguration config = null;
		try {
			config = new PropertiesConfiguration("config/al.properties");
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

		if(config.containsKey("hive_driver_class")) {
			hive_driver_class = config.getString("hive_driver_class");
		}
		if(config.containsKey("hive_db_url")) {
			hive_db_url = config.getString("hive_db_url");
		}
		if(config.containsKey("hive_username")) {
			hive_username = config.getString("hive_username");
		}
		if(config.containsKey("hive_password")) {
			hive_password = config.getString("hive_password");
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

		System.out.println(Config.hive_driver_class);
		System.out.println(Config.hive_db_url);
		System.out.println(Config.hive_username);
		System.out.println(Config.hive_password);

		System.out.println(Config.topic);
		System.out.println(Config.zkHosts);
		System.out.println(Config.brokerList);
	}
}