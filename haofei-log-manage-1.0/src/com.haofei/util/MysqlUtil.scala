package com.haofei.util

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}

import com.haofei.domain.DataSourceTrait
import javax.sql.DataSource

import scala.collection.mutable

object MysqlUtil {

  // 数据保存 -> MySQL
  def saveToMysql(sqls: Array[String],ds:DataSourceTrait) = {
    var conn: Connection = null
    var stat: Statement = null
    var nowTable = ""
    var nextTable = ""
    val arrays = new mutable.ArrayBuffer[String]
    try {
      conn = ds.getConnection()
      stat = conn.createStatement()
      for (i <- 0 until sqls.size) {
        stat.addBatch(sqls(i))
        arrays.append(sqls(i))
        if ( i + 1 < sqls.size){
          nowTable = sqls(i).substring(12, sqls(i).indexOf("("))
          nextTable = sqls(i + 1).substring(12, sqls(i + 1).indexOf("("))
        }
        if ( nowTable != nextTable || arrays.size > 1000 ) {
          try {
            stat.executeBatch()
            stat.clearBatch()
            arrays.clear()
          } catch {
            case e: Exception => {
              arrays.foreach(println)
              e.printStackTrace()
            }
          }
        }
      }
      stat.executeBatch()
    } catch {
      case e: Exception => {
        arrays.foreach(println)
        e.printStackTrace()
      }
    } finally {
      if (stat != null) stat.close()
      if (conn != null) conn.close()
    }
  }

  // 获取对应数据库所有表结构 -> Map(key:表名,value:所有字段)
  def getTableMap(dataBase: String,ds:DataSourceTrait): mutable.HashMap[String, Array[String]] = {
    val tableMap = new mutable.HashMap[String, Array[String]]
    val mutableArray = new mutable.ArrayBuffer[String]()
    var lastTableName = ""
    var tableName = ""
    var columnName = ""

    // 查询该数据库的所有表结构, 表名 : 表字段
    val sql = "select t1.TABLE_NAME as tableName,t1.COLUMN_NAME as columnName " +
      "from information_schema.`COLUMNS` t1 where t1.TABLE_NAME in ( " +
      "select t.TABLE_NAME from information_schema.`COLUMNS`  t where t.TABLE_SCHEMA = '" + dataBase +
      "' and  t.COLUMN_NAME = 'pid') order by t1.TABLE_NAME,t1.ORDINAL_POSITION"

    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        tableName = rs.getString("tableName")
        columnName = rs.getString("columnName")

        if ( tableName != lastTableName && !lastTableName.isEmpty) {
          tableMap.put(lastTableName,mutableArray.toArray)
          mutableArray.clear()
        }

        if (!columnName.equals("pid")) {
          mutableArray.append(columnName)
        }

        lastTableName = tableName
      }
      tableMap.put(tableName,mutableArray.toArray)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    tableMap
  }
}
