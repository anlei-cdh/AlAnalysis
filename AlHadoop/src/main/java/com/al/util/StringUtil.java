package com.al.util;

import java.io.UnsupportedEncodingException;

public class StringUtil {
	/**
	 * 标题和Url截断
	 * @param str
	 * @param len
	 * @param encoding
	 * @return
	 */
	public static String limitString(String str, int len, String encoding) {
		String s = str;
		try {
			byte[] yy = s.getBytes(encoding);
			while(s != null && yy.length > len) {
				int sublen = getEncodingLen(yy.length, s.length(), len);
				if(sublen < 0) {
					sublen = s.length() / 2;
				}
				s = s.substring(0, sublen);
				yy = s.getBytes(encoding);
			}
			return s;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return s;
	}

	private static int getEncodingLen(int bytelen, int strlen, int maxlen) {
		return strlen-(bytelen - maxlen);
	}

	/**
	 * 标题清理过滤
	 * @param title
	 * @return
	 */
	public static String clearTitleAll(String title) {
		String result = clearTitle(title, "-");
		result = clearTitle(result, "_");
		return result;
	}

	public static String clearTitle(String title, String symbol) {
		if(title == null) {
			return "";
		}
		int index = title.lastIndexOf(symbol);
		return index < 5 ? title : clearTitle(title.substring(0, index), symbol);
	}
}
