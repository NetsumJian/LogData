package com.haofei.utils

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import scala.collection.mutable

object C3P0Util {

  // 数据保存 -> MySQL
  def saveToMysql(sqls:Array[String]) = {

    var conn:Connection = null
    var stat:Statement = null
    try{
      conn = AuthDataSource.getConnection()
      stat = conn.createStatement()
      for (i <- 0 until sqls.size){
        stat.addBatch(sqls(i))
        if (i + 1 < sqls.size && sqls(i).substring(12,sqls(i).indexOf("(")) != sqls(i+1).substring(12,sqls(i+1).indexOf("(")) ){
          try{
            stat.executeBatch()
            stat.clearBatch()
          }catch {
            case e: Exception => e.printStackTrace()
          }
        }
      }
      stat.executeBatch()
    }catch{
      case e: Exception => e.printStackTrace()
    }finally {
      if (stat != null ) stat.close()
      if (conn != null ) conn.close()
    }
  }

  //
  def getTableMap(dataBase:String):mutable.HashMap[String,String] ={
    val tableMap = new mutable.HashMap[String,String]

    // 查询数据库的表结构, 表名 : 表字段
    val sql = "select t1.TABLE_NAME as tableName,t1.COLUMN_NAME as columnName " +
      "from information_schema.`COLUMNS` t1 where t1.TABLE_NAME in ( " +
      "select t.TABLE_NAME from information_schema.`COLUMNS`  t where t.TABLE_SCHEMA = '" + dataBase +
      "' and  t.COLUMN_NAME = 'pid') order by t1.TABLE_NAME,t1.ORDINAL_POSITION"

    var conn:Connection = null
    var ps:PreparedStatement = null
    var rs:ResultSet = null

    try{
      conn = AuthDataSource.getConnection()
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      // (first_not_show_bind_flow,event_time,mfr_id,account_id,channel_id)
      while (rs.next()){
        val tableName = rs.getString("tableName")
        val columnName = "`"+rs.getString("columnName")+"`"
        if (!tableMap.contains(tableName))
          tableMap.put(tableName,columnName)
        else if (!columnName.equals("`pid`")){
          val tableValue = tableMap(tableName) +","+ columnName
          tableMap.put(tableName,tableValue)
        }
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }

    tableMap
  }

}
