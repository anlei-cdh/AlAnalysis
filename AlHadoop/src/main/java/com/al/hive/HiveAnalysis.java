package com.al.hive;

import com.al.basic.BasicDao;
import com.al.config.Config;
import com.al.dao.DimensionDao;
import com.al.db.DBHelper;
import com.al.entity.Dimension;

import java.sql.Connection;

/**
 * Created by An on 2016/11/28.
 */
public class HiveAnalysis {

    public static Dimension getHiveDimension(Dimension dimension) {
        Dimension result = new Dimension();
        Connection conn = DBHelper.getHiveConnection();
        try {
            String sql = "select count(*) pv,count(distinct(uuid)) uv,count(distinct(ip)) ip from al " +
                         "where day = #{day} and ip is not null and uuid is not null";
            result = (Dimension)BasicDao.getSqlObject(sql, dimension, conn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBHelper.close(conn);
        }
        return result;
    }

    public static void saveHiveDimensionData(Dimension dimension) {
        Connection conn = DBHelper.getConnection();
        try {
            DimensionDao.saveHiveDimensionData(dimension, conn);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DBHelper.close(conn);
        }
    }

    public static void runAnalysis() {
        Dimension dimension = new Dimension();
        dimension.setDay(Config.day.replace("-",""));
        Dimension result = getHiveDimension(dimension);
        result.setDay(Config.day);
        saveHiveDimensionData(result);
    }

    public static void main(String[] args) {
        HiveAnalysis.runAnalysis();
    }
}
