package com.haofei.machine

import com.haofei.domain.TestDataSource
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object LostPlayerDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("LostPlayer")
      .getOrCreate()

    val conn = TestDataSource.getConnection()
    val ps = conn.prepareStatement(
      """
        |SELECT t1.a,t1.v,t1.f,t1.c,t1.o,IF(t2.account_id IS NULL,1,0)
        |FROM (
        |SELECT t.account_id a , max(t.vip) v ,max(t.factor) f , COUNT(1) c ,AVG(t.online_time) o
        |FROM (
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-06`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-07`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-08`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-09`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-10`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-11`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-12`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-13`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-14`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-15`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-16`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020a-05-17`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-18`
        |UNION ALL
        |SELECT account_id,vip,register_time,factor,online_time FROM `active_player2020-05-19`
        |) t
        |WHERE t.register_time < UNIX_TIMESTAMP('2020-05-06')
        |GROUP BY t.account_id ) t1
        |LEFT JOIN data_tslog.login_flow t2 ON t1.a = t2.account_id
        |AND t2.event_time BETWEEN UNIX_TIMESTAMP('2020-05-20') AND UNIX_TIMESTAMP('2020-06-05')
        |GROUP BY t1.a
        |ORDER BY t1.a
        |""".stripMargin)
    val rs = ps.executeQuery()
    val arr = new mutable.ArrayBuffer[(String,String,String,String,String)]()
    while (rs.next()){
      val id = rs.getString(1)
      val vip = rs.getString(2)
      val factor = rs.getString(3)
      val nums = rs.getString(4)
      val times = rs.getString(5)
      arr.append((id,vip,factor,nums,times))
    }
    arr.take(10).foreach(println)

  }

}
