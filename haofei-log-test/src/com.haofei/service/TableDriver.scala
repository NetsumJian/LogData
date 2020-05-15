package com.haofei.service

import com.haofei.utils.{AuthDataSource, C3P0Util, MysqlUtil}
import org.apache.spark.{SparkConf, SparkContext}

object TableDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("hiveSql")
    val sc = new SparkContext(conf)

    val tableMap = MysqlUtil.getTableMap("data_tslog")
    val hiveSql = tableMap.map{ x =>
      val table = x._1
      // val value = x._2.mkString(",")
      "alter table "+table + " add partition( report_time='${pTime}') location '/hive_tslog/"+table+"/reportTime=${pTime}' ; "
    }.toArray
    // hiveSql.sorted
    sc.makeRDD(hiveSql.sorted).saveAsTextFile("D://hivesql")
  }

}
