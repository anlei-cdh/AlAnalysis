package com.al.dao

import com.al.basic.BasicSimpleDao
import com.al.entity.Dimension

import scala.collection.mutable.ListBuffer

/**
  * Created by An on 2016/11/25.
  */
object DimensionDao {
  /**
    * 查询维度配置表
    * @param dimension
    * @return
    */
  def getDimensionConfig(dimension: Dimension): ListBuffer[Dimension] = {
    val sql: String = "SELECT id dimeId,`value` FROM common_dimension WHERE `type` = #{type}"
    return BasicSimpleDao.getSqlList(sql, dimension).asInstanceOf[ListBuffer[Dimension]]
  }

  /**
    * 获得维度配置表映射
    * @param dimension
    * @return
    */
  def getDimensionConfigMap(dimension: Dimension): Map[String , Dimension] = {
    val list: ListBuffer[Dimension] = getDimensionConfig(dimension)
    var map: Map[String , Dimension] = Map[String , Dimension]()
    for(dimension <- list) {
      map += (dimension.value -> dimension)
    }
    return map
  }

  /**
    * 搜索引擎映射
    * @return
    */
  def getSearchEngineMap(): Map[String , Dimension] = {
    val dimension: Dimension = new Dimension
    dimension.`type` = "Search-engine"
    return DimensionDao.getDimensionConfigMap(dimension)
  }

  /**
    * 国家映射
    * @return
    */
  def getCountryMap(): Map[String , Dimension] = {
    val dimension: Dimension = new Dimension
    dimension.`type` = "country"
    return DimensionDao.getDimensionConfigMap(dimension)
  }

  /**
    * 省份映射
    * @return
    */
  def getProvinceMap(): Map[String , Dimension] = {
    val dimension: Dimension = new Dimension
    dimension.`type` = "province"
    return DimensionDao.getDimensionConfigMap(dimension)
  }

  /**
    * 流量统计写库
    * @param dimension
    * @return
    */
  def saveDimensionData(dimension: Dimension): Int = {
    val sql: String = "insert into sparkcore_dimension_data(dimeid,`day`,pv,uv,ip,time) values (#{dimeId},#{day},#{pv},#{uv},#{ip},#{time}) on duplicate key update pv = values(pv),uv = values(uv),ip = values(ip),time = values(time)"
    return BasicSimpleDao.saveObject(sql, dimension)
  }

  /**
    * 维度信息写库
    * @param list
    * @return
    */
  def saveDimensionList(list: List[Dimension]): Int = {
    val sql: String = "insert into sparkcore_dimension_data(dimeid,`day`,pv,uv,ip,time) values (#{dimeId},#{day},#{pv},#{uv},#{ip},#{time}) on duplicate key update pv = values(pv),uv = values(uv),ip = values(ip),time = values(time)"
    return BasicSimpleDao.saveListBatch(sql, list)
  }

  /**
    * 实时维度信息写库
    * @param list
    * @return
    */
  def saveStreamingDimensionList(list: List[Dimension]): Int = {
    val sql: String = "insert into streaming_dimension_data(dimeid,`second`,pv,uv) values (#{dimeId},#{second},#{pv},#{uv}) on duplicate key update pv = values(pv),uv = values(uv)"
    return BasicSimpleDao.saveListBatch(sql, list)
  }

  def main(args: Array[String]): Unit = {

  }
}
