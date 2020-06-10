package com.haofei.util

import java.sql.{Connection, PreparedStatement, ResultSet}

import com.haofei.domain.DataSourceTrait

import scala.collection.mutable

object MysqlUtil {

  def saveSingleTable(sqls: Array[String],ds:DataSourceTrait): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val arrays = new mutable.ArrayBuffer[String]
    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement("set names utf8mb4")
      ps.execute()
      for (i <- 0 until sqls.size) {
        ps.addBatch(sqls(i))
        arrays.append(sqls(i))
        if ( arrays.size > 1000 ) {
          try {
            ps.executeBatch()
            ps.clearBatch()
            arrays.clear()
          } catch {
            case e: Exception => {
              arrays.foreach(println)
              EmailUtil.sendSimpleTextEmail("Mysql玩家流失预测数据入库异常",
                s"""数据库 : ${ds.getClass}
                   |数据 : ${arrays(0)}
                   |异常原因 : ${e.getMessage}
                   |${e.printStackTrace}
                   |""".stripMargin)
            }
          }
        }
      }
      ps.executeBatch()
    } catch {
      case e: Exception => {
        arrays.foreach(println)
        EmailUtil.sendSimpleTextEmail("Mysql玩家流失预测数据入库异常",
          s"""数据库 : ${ds.getClass}
             |数据 : ${arrays(0)}
             |异常原因 : ${e.getMessage}
             |${e.printStackTrace}
             |""".stripMargin)
      }
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
  }

  def getTraintArray(sql:String , ds:DataSourceTrait) :mutable.ArrayBuffer[(Int,Int,Int,Double,Double,Double)] = {
    val traintArray = new mutable.ArrayBuffer[(Int,Int,Int,Double,Double,Double)]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val id = rs.getInt("id")
        val vip = rs.getInt("vip")
        val factor = rs.getInt("factor")
        val nums = rs.getInt("nums")
        val atime = rs.getDouble("atime")
        val la = rs.getInt("la")
        traintArray.append((id, vip, factor, nums, atime, la))
      }
    } catch {
      case e => {
        EmailUtil.sendSimpleTextEmail("Mysql获取数据训练集异常",
          s"""数据库 : ${ds.getClass}
             |异常原因 : ${e.getMessage}
             |${e.printStackTrace}
             |""".stripMargin)
      }
    }finally {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    traintArray
  }

  def getTestArray(sql:String , ds:DataSourceTrait) :mutable.ArrayBuffer[(Int,Int,Int,Double,Double)] = {
    val testArray = new mutable.ArrayBuffer[(Int,Int,Int,Double,Double)]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        val id = rs.getInt("id")
        val vip = rs.getInt("vip")
        val factor = rs.getInt("factor")
        val nums = rs.getInt("nums")
        val atime = rs.getDouble("atime")
        testArray.append((id, vip, factor, nums, atime))
      }
    } catch {
      case e => {
        EmailUtil.sendSimpleTextEmail("Mysql获取数据测试集异常",
          s"""数据库 : ${ds.getClass}
             |异常原因 : ${e.getMessage}
             |${e.printStackTrace}
             |""".stripMargin)
      }
    }finally {
      if (rs != null) rs.close()
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
    testArray
  }

}
