package com.al.basic

import java.lang.reflect.Method
import java.sql.{Connection, PreparedStatement, ResultSet, ResultSetMetaData}

import com.al.db.DBHelper
import org.apache.commons.lang3.StringUtils

import scala.collection.mutable.ListBuffer

object BasicDao {
  
  /**
	 * 根据Sql和实体参数 查询数据库 返回结果集的第一个实体对象
	 * 1. setPreparedSql方法是对预置语句进行处理
	 * 2. getRsListFromMetaData方法是对结果集自动填充容器
	 * 3. 实体类里面需要调用的方法必须是对象类型 即int -> Integer
	 * @param sql
	 * @param conn
	 * @param entity
	 * @return
	 */
	def getSqlObject(sql: String, entity: Any, conn: Connection): Any = {
		var list: ListBuffer[Any] = getSqlList(sql, entity, conn)
		if(list != null && list.size > 0) {
			return list(0)
		}
		return null
	}
	
	/**
	 * 根据Sql和实体参数 查询数据库 返回结果集
	 * 1. setPreparedSql方法是对预置语句进行处理
	 * 2. getRsListFromMetaData方法是对结果集自动填充容器
	 * 3. 实体类里面需要调用的方法必须是对象类型 即int -> Integer
	 * @param sql
	 * @param entity
	 * @param conn
	 * @return
	 */
	def getSqlList(sql: String, entity: Any, conn: Connection): ListBuffer[Any] = {
		var cls: Class[_] = entity.getClass()
		var pstmt: PreparedStatement = null
		var rs: ResultSet = null
		try {
			pstmt = conn.prepareStatement(getRealSql(sql))
			setPreparedSql(sql, pstmt, entity)
			rs = pstmt.executeQuery()
			return getRsListFromMetaData(rs, cls)
		} finally {
			DBHelper.close(rs)
			DBHelper.close(pstmt)
		}
	}
	
	/**
	 * 无where条件查询数据库 返回结果集
	 * 1. 最普通的查询不需要解析#{}里面的内容
	 * 2. getRsListFromMetaData方法是对结果集自动填充容器
	 * 3. 实体类里面需要调用的方法必须是对象类型 即int -> Integer
	 * @param sql
	 * @param cls
	 * @param conn
	 * @return
	 */
	def getSqlList(sql: String, cls: Class[_], conn: Connection): ListBuffer[Any] = {
		var pstmt: PreparedStatement = null
		var rs: ResultSet = null
		try {
			pstmt = conn.prepareStatement(sql)
			rs = pstmt.executeQuery()
			return getRsListFromMetaData(rs, cls)
		} finally {
			DBHelper.close(rs)
			DBHelper.close(pstmt)
		}
	}
	
	/**
	 * 保存数据
	 * @param sql
	 * @param entity
	 * @param conn
	 */
	def saveObject(sql: String, entity: Any, conn: Connection): Int = {
		return executeSql(sql, entity, conn)
	}
	
	/**
	 * 保存数据集合
	 * @param sql
	 * @param entities
	 * @param conn
	 */
	def saveList(sql: String, entities: List[Any], conn: Connection): Int = {
		var count: Int = 0
		for(entity: Any <- entities) {
			count += saveObject(sql, entity, conn)
		}
		return count
	}
	
	/**
	 * 批处理保存数据集合
	 * @param sql
	 * @param entities
	 * @param conn
	 */
	def saveListBatch(sql: String, entities: List[Any], conn: Connection): Int = {
		DBHelper.setAutoCommit(conn, false)
		var pstmt: PreparedStatement = null
		try {
			pstmt = conn.prepareStatement(getRealSql(sql))
			var count: Int = 0
			for(entity: Any <- entities) {
				setPreparedSql(sql, pstmt, entity)
				pstmt.addBatch()
				count += 1
				DBHelper.executeBatch(conn, pstmt, count)
			}
			pstmt.executeBatch()
			DBHelper.commit(conn)
			return count
		} finally {
			DBHelper.close(pstmt)
			DBHelper.setAutoCommit(conn, true)
		}
	}
	
	/**
	 * 删除数据
	 * @param sql
	 * @param entity
	 * @param conn
	 */
	def deleteObject(sql: String, entity: Any, conn: Connection): Int = {
		return executeSql(sql, entity, conn)
	}
	
	/**
	 * 修改数据
	 * @param sql
	 * @param entity
	 * @param conn
	 */
	def updateObject(sql: String, entity: Any, conn: Connection): Int = {
		return executeSql(sql, entity, conn)
	}
	
	/**
	 * 修改数据集合
	 * @param sql
	 * @param entities
	 * @param conn
	 */
	def updateList(sql: String, entities: List[Any], conn: Connection): Int = {
		var count: Int = 0
		for(entity: Any <- entities) {
			count += updateObject(sql, entity, conn)
		}
		return count
	}
	
	/**
	 * 用MetaData和反射，对结果集进行处理，返回集合
	 * @param rs
	 * @param cls
	 * @return
	 * @throws NoSuchFieldException 
	 */
	def getRsListFromMetaData(rs: ResultSet, cls: Class[_]): ListBuffer[Any] = {
		var list: ListBuffer[Any] = ListBuffer[Any]()
		// MetaData
		var data: ResultSetMetaData = rs.getMetaData()
		var columnCount: Int = data.getColumnCount()
		
		while(rs.next()){
			var clsInstance: Any = cls.newInstance()
			for (i <- 1 to columnCount) {
			  // MetaData columnName
        var columnName: String = data.getColumnLabel(i) // data.getColumnName(i)
        // GetColumn return type
        var typeCls: Class[_] = cls.getMethod("get" + toFirstUpperCase(columnName)).getReturnType()
        // SetColumn
  		  var method: Method = cls.getMethod("set" + toFirstUpperCase(columnName), typeCls)
  		  // Integer String Float
  			var simpleName: String = toFirstUpperCase(typeCls.getSimpleName())
  			// 对Integer类型的特殊处理
  			if(simpleName.equalsIgnoreCase("Integer")) {
  			  simpleName = "Int"
  			}
  			// Rs getString getInt getFloat
  			var rsMethod: Method = rs.getClass().getMethod("get" + simpleName, classOf[String])
  			// Rs getString getInt getFloat invoke
  			var value: Object = rsMethod.invoke(rs, columnName)
  			// SetColumn invoke
  			method.invoke(clsInstance, value)
      }
			list.append(clsInstance)
		}
		return list
	}
	
	/**
	 * 对带#{xxx}的sql语句进行处理，自动添加pstmt.setString(1, ...)等
	 * @param sql
	 * @param pstmt
	 * @param entity
	 */
	def setPreparedSql(sql: String, pstmt: PreparedStatement, entity: Any): Unit = {
		if(sql.contains("#")) {
			var cls: Class[_] = entity.getClass()
			var sqlSplit: Array[String] = sql.split("#")
			for(i <- 1 until sqlSplit.length) {
				var split: String = sqlSplit(i)
				var splitbefore: String = StringUtils.substringBeforeLast(split, "}").trim()
				var paramName: String = StringUtils.substringAfterLast(splitbefore, "{").trim()
				
				// getXxxx
				var paramMethod: Method = cls.getMethod("get" + toFirstUpperCase(paramName))
				// getXxxx invoke
				var paramValue: Any = paramMethod.invoke(entity)
				pstmt.setString(i, paramValue.toString())
			}
		}
	}
	
	/**
	 * 得到可以运行的sql,将#{xxx}的部分替换成?
	 * @param sql
	 */
	def getRealSql(sql: String): String = {
		return sql.replaceAll("\\#\\{ *[a-z,A-Z,0-9,_]+ *\\}", "\\?")
	}
	
	/**
	 * 根据Sql和实体参数 对数据库进行 增,删,改 操作
	 * setPreparedSql方法是对预置语句进行处理
	 * @param sql
	 * @param entity
	 * @param conn
	 */
	def executeSql(sql: String, entity: Any, conn: Connection): Int = {
		var pstmt: PreparedStatement = null
		try {
			pstmt = conn.prepareStatement(getRealSql(sql))
			setPreparedSql(sql, pstmt, entity)
			return pstmt.executeUpdate()
		} finally {
			DBHelper.close(pstmt)
		}
	}
	
	/**
	 * 根据Sql对数据库进行 增,删,改 操作
	 * @param sql
	 * @param conn
	 */
	def executeSql(sql: String, conn: Connection): Int = {
		var pstmt: PreparedStatement = null
		try {
			pstmt = conn.prepareStatement(sql)
			return pstmt.executeUpdate()
		} finally {
			DBHelper.close(pstmt)
		}
	}
	
	/**
	 * 字符串首字母大写
	 * @param str
	 * @return 首字母大写的字符串
	 */
	def toFirstUpperCase(str: String): String = {
		if(str == null || str.length() < 1) {
			return ""
		}
		val start: String = str.substring(0,1).toUpperCase()
		val end: String = str.substring(1, str.length())
		return start + end
	}
  
  def main(args: Array[String]): Unit = {
    
  }
}