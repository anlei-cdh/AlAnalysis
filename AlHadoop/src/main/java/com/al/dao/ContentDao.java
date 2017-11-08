package com.al.dao;

import com.al.basic.BasicDao;
import com.al.entity.Content;

import java.sql.Connection;
import java.util.List;

public class ContentDao {
	public static int saveStormContentData(List<Content> list, Connection conn) throws Exception {
		String sql = "insert into storm_content_data(contentId,`second`,pv,uv) values (#{contentId},#{second},#{pv},#{uv}) " +
					 "on duplicate key update pv = values(pv),uv = values(uv)";
		return BasicDao.saveListBatch(sql, list, conn);
	}
	
	public static int truncateStormContentData(Connection conn) throws Exception {
		String sql = "TRUNCATE storm_content_data";
		return BasicDao.executeSql(sql, conn);
	}
	
	public static int saveStormContentDetail(List<Content> list, Connection conn) throws Exception {
		String sql = "insert into storm_content_detail(contentId,url,title) values (#{contentId},#{url},#{title}) " +
				     "on duplicate key update url = values(url),title = values(title)";
		return BasicDao.saveListBatch(sql, list, conn);
	}
	
	public static int truncateStormContentDetail(Connection conn) throws Exception {
		String sql = "TRUNCATE storm_content_detail";
		return BasicDao.executeSql(sql, conn);
	}
}
