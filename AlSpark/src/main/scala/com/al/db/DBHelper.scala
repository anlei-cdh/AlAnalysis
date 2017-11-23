package com.al.db

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.al.basic.BasicDao

object DBHelper {
  
  /**
   * 获得默认的数据库连接
   */
  def getConnection(): Connection = {
    // return JDBCHelper.getConnection()
		return C3P0Helper.getConnection()
	}

	/**
		* 获得事物的数据库连接
		*/
	def getConnectionAtFalse(): Connection = {
		val conn: Connection = DBHelper.getConnection()
		DBHelper.setAutoCommit(conn, false)
		return conn
	}

	/**
		* 添加批处理操作并执行
		*/
	def setPreparedSqlexecuteBatch(conn: Connection, pstmt: PreparedStatement, sql: String, count: Int, entity: Any): Unit = {
		BasicDao.setPreparedSql(sql, pstmt, entity)
		pstmt.addBatch()
		DBHelper.executeBatch(conn, pstmt, count)
	}

	/**
		* 提交并关闭
		*/
	def commitClose(conn: Connection, pstmt: PreparedStatement): Unit = {
		pstmt.executeBatch()
		DBHelper.commit(conn)

		DBHelper.close(pstmt)
		DBHelper.setAutoCommit(conn, true)
		DBHelper.close(conn)
	}

  /**
	 * 事务提交
	 */
	def commit(conn: Connection): Unit = {
	  if (conn != null){
	    conn.commit()
			println("do commit...")
	  }
	}
	
	/**
	 * 事务处理
	 */
	def setAutoCommit(conn: Connection, autoCommit: Boolean): Unit = {
	  if (conn != null) {
	    conn.setAutoCommit(autoCommit)
	  }
	}
	
	/**
	 * 批处理提交
	 */
	def executeBatch(conn: Connection, pstmt: PreparedStatement, count: Int): Unit = {
	  if(count % 1000 == 0) {
	    pstmt.executeBatch()
	    DBHelper.commit(conn)
  	} 
	}
	
	/**
	 * 释放rs stmt conn
	 */
	def close(conn: Connection, pstmt: PreparedStatement, rs: ResultSet): Unit = {
	  close(rs)
		close(pstmt)
		close(conn)
	}
	
	/**
	 * 释放conn
	 */
	def close(conn: Connection): Unit = {
	  if (conn != null) {
	    conn.close()
	  }
	}
	
	/**
	 * 释放pstmt
	 */
	def close(pstmt: PreparedStatement): Unit = {
	  if (pstmt != null) {
	    pstmt.close()
	  }
	}
	
	/**
	 * 释放rs
	 */
	def close(rs: ResultSet): Unit = {
	  if (rs != null) {
	    rs.close()
	  }
	}
  
  def main(args: Array[String]): Unit = {
    
  }
}