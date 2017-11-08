package com.al.dao;

import com.al.basic.BasicDao;
import com.al.db.C3P0Helper;
import com.al.db.DBHelper;
import com.al.entity.Dimension;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DimensionDao {
	
	public static int saveStormDimensionData(List<Dimension> list, Connection conn) throws Exception {
		String sql = "insert into storm_dimension_data(dimeid,`second`,pv,uv) values (#{dimeId},#{second},#{pv},#{uv}) on duplicate key update pv = values(pv),uv = values(uv)";
		return BasicDao.saveListBatch(sql, list, conn);
	}

	public static List<Dimension> getDimensionConfig(Connection conn) throws Exception {
		String sql = "SELECT id dimeId,`type`,`value` FROM common_dimension WHERE `type` = 'country'";
		Dimension dimension = new Dimension();
		return (List<Dimension>)BasicDao.getSqlList(sql, dimension, conn);
	}
	
	public static Map<String,Dimension> getDimensionConfigMap() {
		Map<String,Dimension> map = new HashMap<String,Dimension>();
		Connection conn = C3P0Helper.getConnection();
		try {
			List<Dimension> list = getDimensionConfig(conn);
			for(Dimension dimension : list) {
				map.put(dimension.getValue(), dimension);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBHelper.close(conn);
		}
		return map;
	}
	
	public static int truncateStormDimensionData(Connection conn) throws Exception {
		String sql = "TRUNCATE storm_dimension_data";
		return BasicDao.executeSql(sql, conn);
	}

	public static int saveHiveDimensionData(Dimension dimension, Connection conn) throws Exception {
		String sql = "insert into hive_dimension_data(`day`,pv,uv,ip,time) values (#{day},#{pv},#{uv},#{ip},#{time}) on duplicate key update pv = values(pv),uv = values(uv),ip = values(ip)";
		return BasicDao.saveObject(sql, dimension, conn);
	}

	public static int saveMapreduceDimensionData(Dimension dimension, Connection conn) throws Exception {
		String sql = "insert into hive_dimension_data(`day`,pv,uv,ip,time) values (#{day},#{pv},#{uv},#{ip},#{time}) on duplicate key update time = values(time)";
		return BasicDao.saveObject(sql, dimension, conn);
	}
}
