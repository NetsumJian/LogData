package com.haofei.service

import com.haofei.domain.{LocalDataSource, TestDataSource}
import com.haofei.utils.{AuthDataSource, C3P0Util, EmailUtil, MysqlUtil, MysqlUtil2}
import org.apache.spark.{SparkConf, SparkContext}

object TableDriver {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf().setMaster("local").setAppName("hiveSql")
    val sc = new SparkContext(conf)
    val broad = sc.broadcast(Array(TestDataSource))
    val tableMap = MysqlUtil2.getTableMap("data_tslog",broad.value(0))
    tableMap.take(10).foreach{x=>

    }*/

    try {
      val i = 1 / 0
    } catch {
      case  e =>{
        EmailUtil.sendSimpleTextEmail("test",s"${e.getStackTrace.mkString("\n")}")
      }
    }
    /*val hiveSql = tableMap.map{ x =>
      val table = x._1
      // val value = x._2.mkString(",")
      "alter table "+table + " add partition( report_time='${pTime}') location '/hive_tslog/"+table+"/reportTime=${pTime}' ; "
    }.toArray
    // hiveSql.sorted
    sc.makeRDD(hiveSql.sorted).saveAsTextFile("D://hivesql")*/

  }

}
