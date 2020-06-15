package com.haofei.util

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.haofei.domain.DataSourceTrait
import org.slf4j.LoggerFactory

import scala.collection.mutable

object JdbcUtil {

  val logger = LoggerFactory.getLogger(JdbcUtil.getClass)
  // 数据保存 -> MySQL
  def saveToMysql(sqls: Array[String],ds:DataSourceTrait) = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var nowTable = ""
    var nextTable = ""
    val arrays = new mutable.ArrayBuffer[String]
    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement("set names utf8mb4")
      ps.execute()
      for (i <- 0 until sqls.size) {
        ps.addBatch(sqls(i))
        arrays.append(sqls(i))
        if ( i + 1 < sqls.size){
          nowTable = sqls(i).substring(12, sqls(i).indexOf("("))
          nextTable = sqls(i + 1).substring(12, sqls(i + 1).indexOf("("))
        }
        if ( nowTable != nextTable || arrays.size > 1000 ) {
          try {
            ps.executeBatch()
            ps.clearBatch()
            arrays.clear()
          } catch {
            case e: Exception => {
              val msg = e.getMessage
              val c = ds.getClass.toString
              val stackTrace = e.getStackTrace.mkString("\n")

              arrays.foreach{s =>
                logger.error(s)
              }

              logger.error(c)
              logger.error(msg)
              logger.error(stackTrace)

              EmailUtil.sendSimpleTextEmail("Mysql数据入库异常",
                s"""数据库 : $c
                   |数据 : ${arrays(0)}
                   |异常原因 : $msg
                   |$stackTrace
                   |""".stripMargin)
            }
          }
        }
      }
      ps.executeBatch()
    } catch {
      case e: Exception => {
        val msg = e.getMessage
        val c = ds.getClass.toString
        val stackTrace = e.getStackTrace.mkString("\n")

        arrays.foreach{s =>
          logger.error(s)
        }

        logger.error(c)
        logger.error(msg)
        logger.error(stackTrace)

        EmailUtil.sendSimpleTextEmail("Mysql数据入库异常",
          s"""数据库 : $c
             |数据 : ${arrays(0)}
             |异常原因 : $msg
             |$stackTrace
             |""".stripMargin)
      }
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
  }

  // 获取对应数据库所有表结构 -> Map(key:表名,value:所有字段)
  def getTableMap(dataBase: String,ds:DataSourceTrait): mutable.HashMap[String, Array[String]] = {
    val tableMap = new mutable.HashMap[String, Array[String]]
    val mutableArray = new mutable.ArrayBuffer[String]
    var lastTableName = ""
    var tableName = ""
    var columnName = ""

    // 查询该数据库的所有表结构, 表名 : 表字段
    val sql = "select t1.TABLE_NAME as tableName,t1.COLUMN_NAME as columnName " +
      "from information_schema.`COLUMNS` t1 where t1.TABLE_SCHEMA = '" + dataBase + "'" +
      "order by t1.TABLE_NAME"

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
        EmailUtil.sendSimpleTextEmail("Mysql获取表结构异常",
          s"""数据库 : ${ds.getClass}
             |异常原因 : ${e.getMessage}
             |${e.getStackTrace.mkString("\n")}
             |""".stripMargin)
      }
    } finally {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    tableMap
  }

}
