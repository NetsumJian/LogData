package com.haofei.service

import com.haofei.domain.{LocalDataSource, SXGFDataSource, SXQDDataSource, TTGFDataSource, TTQDDataSource, TestDataSource}
import com.haofei.utils.{AuthDataSource, C3P0Util, EmailUtil, MysqlUtil}
import org.apache.spark.{SparkConf, SparkContext}

object TableDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hiveSql")
    val sc = new SparkContext(conf)
    val dsValue = sc.broadcast(Array(SXGFDataSource,SXQDDataSource,TTGFDataSource,TTQDDataSource))
    val tableMap = MysqlUtil.getTableMap("data_tslog",dsValue.value(2))
    tableMap.take(10).foreach(println)

    val hiveSql = tableMap.map{ x =>
      val table = x._1
      "alter table "+table + " add partition( report_time='${pTime}') location '/hive_tslog/sxgf/"+table+"/report_time=${pTime}' ; "
    }.toList

    sc.makeRDD(hiveSql.sorted).saveAsTextFile("D:\\Document\\WorkDoc\\hive\\ttgf")

  }

}
