package com.al.util

object StringUtil {

	/**
		* 标题和Url截断
		* @param str
		* @param len
		* @param encoding
		* @return
		*/
	def limitString(str: String , len: Int , encoding: String): String = {
		  var s = str
			var yy: Array[Byte] = s.getBytes(encoding);
			while(s != null && yy.length > len) {
				var sublen: Int  = getEncodingLen(yy.length, s.length(), len);
				if(sublen < 0) {
					sublen = s.length() / 2;
				}
				s = s.substring(0, sublen);
				yy = s.getBytes(encoding);
			}
			return s;
	}
	
	def getEncodingLen(bytelen: Int, strlen: Int , maxlen: Int): Int = {
		return strlen - (bytelen - maxlen);
	}

	/**
		* 标题清理过滤
		* @param title
		* @return
		*/
	def clearTitleAll(title: String): String = {
		var result: String = clearTitle(title, "-")
		result = clearTitle(result, "_")
		return result
	}

	def clearTitle(title: String, symbol: String): String = {
		if(title == null) {
			return ""
		}
		val index: Int = title.lastIndexOf(symbol)
		if(index < 5) {
			return title
		} else {
			return clearTitle(title.substring(0, index), symbol)
		}
	}
}