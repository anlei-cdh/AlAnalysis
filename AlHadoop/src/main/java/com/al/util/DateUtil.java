package com.al.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {
	
	/**
	 * 得到当前时间在一天中的秒数
	 * @param addSecond
	 * @return
	 */
	public static int getSecond(int addSecond) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.SECOND, addSecond);
		
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);
        int secondOfHour = calendar.get(Calendar.SECOND);
        int second = hour * 3600 + minute * 60 + secondOfHour;
        return second;
	}
	
	/**
	 * 得到当前时间的前n天或后n天的日期的日期
	 * @return
	 */
	public static String getDay(int addday) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, addday);
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(calendar.getTime());
	}

	
	public static void main(String arg[]) {
		System.out.println(DateUtil.getSecond(0));
		System.out.println(DateUtil.getDay(-2));
	}
}