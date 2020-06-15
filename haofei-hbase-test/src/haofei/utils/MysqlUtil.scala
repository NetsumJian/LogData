package haofei.utils

import java.sql.{Connection, PreparedStatement, ResultSet}

import haofei.domain.{ActivePlayer, DataSourceTrait}

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

  def getPlayerArray(date:String , ds:DataSourceTrait) :mutable.ArrayBuffer[ActivePlayer] = {
    val sql =
      s"""
         |SELECT account_id,uid,channel_id,register_time,
         |pay_amount,today_pay_amount,factor,energy_factor,
         |gold,pay_gold,diamond,lottery,energy,
         |`level`,vip,red_packet,bankruptcy_num,room_id,fire_times
         |FROM `active_player$date`""".stripMargin
    val playerArray = new mutable.ArrayBuffer[ActivePlayer]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null
    try {
      conn = ds.getConnection()
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()
      while (rs.next()) {
        var id = rs.getString(1)
        if (id.length < 11) {
          for ( i <- 0 until (11-id.length) ){
            id = "0" + id
          }
        }
        val player = ActivePlayer(id,
          rs.getString(2),
          rs.getString(3),
          rs.getString(4),
          rs.getString(5),
          rs.getString(6),
          rs.getString(7),
          rs.getString(8),
          rs.getString(9),
          rs.getString(10),
          rs.getString(11),
          rs.getString(12),
          rs.getString(13),
          rs.getString(14),
          rs.getString(15),
          rs.getString(16),
          rs.getString(17),
          rs.getString(18),
          rs.getString(19),
          date
        )
        playerArray.append(player)
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
    playerArray
  }


}
