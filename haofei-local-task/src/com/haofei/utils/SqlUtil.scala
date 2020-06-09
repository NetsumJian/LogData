package com.haofei.utils

import java.util

import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object SqlUtil {

  def getTraintSql():String ={
    val traintDate = new DateTime().toLocalDate-15.day
    val list = new util.ArrayList[LocalDate]()
    for ( i <- 0 until 14 ){
      list.add(traintDate - i.day)
    }
    val sql =
      s"""SELECT t1.a id,t1.v vip,t1.f factor,t1.c nums,t1.o atime,IF(t2.account_id IS NULL,1,0) la
        |FROM (
        |SELECT t.account_id a , max(t.vip) v ,max(t.factor) f , COUNT(1) c ,AVG(t.online_time) o
        |FROM (
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(0)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(1)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(2)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(3)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(4)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(5)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(6)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(7)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(8)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(9)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(10)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(11)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(12)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(13)}`
        |) t
        |WHERE t.register_time < UNIX_TIMESTAMP('${list.get(13)}')
        |GROUP BY t.account_id ) t1
        |LEFT JOIN (
        |SELECT account_id
        |FROM data_tslog.login_flow
        |WHERE event_time BETWEEN UNIX_TIMESTAMP('${traintDate+1.day}') AND UNIX_TIMESTAMP('${traintDate+15.day}')
        |GROUP BY account_id
        |) t2 ON t1.a = t2.account_id
        |GROUP BY t1.a
        |ORDER BY t1.a
        |""".stripMargin
    sql
  }

  def getPredictSql():String = {
    val yesterday = new DateTime().toLocalDate-1.day
    val list = new util.ArrayList[LocalDate]()
    for ( i <- 0 until 14 ){
      list.add(yesterday - i.day)
    }
    val sql =
      s"""SELECT t.account_id id , max(t.vip) vip ,max(t.factor) factor , COUNT(1) nums ,AVG(t.online_time) atime
        |FROM (
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(0)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(1)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(2)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(3)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(4)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(5)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(6)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(7)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(8)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(9)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(10)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(11)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(12)}` UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player${list.get(13)}`
        |) t
        |WHERE t.register_time < UNIX_TIMESTAMP('${list.get(13)}')
        |GROUP BY t.account_id
        |""".stripMargin
    sql
  }

}
