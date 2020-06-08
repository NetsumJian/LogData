package com.haofei.util

import java.net.URLDecoder

import org.apache.spark.rdd.RDD

import scala.collection.mutable

object RddUtil {
  def rddToSql(rdd:RDD[String],tableMap:mutable.HashMap[String,Array[String]]): RDD[String] ={
    // 数据过滤解析 -> 去除异常数据
    val parsedata = rdd
      .map(_.split("\\|"))
      .sortBy(_ (0))
    // 数据处理 Array[String] -> sql语句
    val sqldata = parsedata
      .filter(a => tableMap.contains(a(0)))
      .map { arr =>
        val tableName = arr(0)
        val columnArray = tableMap(tableName)
        var str = ""
        val size = if (arr.size -1 > columnArray.size) columnArray.size else arr.size-1
        for (i <- 0 until size + 1) {
          if (i == 0) {
            // insert into guess_server_record_flow(
            str = "insert into " + tableName + "("
            for (j <- 0 until size){
              // `event_time`,`room_type`,`room_id`,`stage`,`red_cards`,`blue_cards`,`red_result`,`blue_result`
              str = str + "`" + columnArray(j) + "`,"
            }
            // ) values (
            str = str.dropRight(1) + ") values ("
          }
          else
          // '1586844573','1','131','1586844573','[6,7,8,8,9]','[1,1,1,1,10]','-4','4'
            str = str + "'" + arr(i) + "',"
        }
        // );
        str = str.dropRight(1) + ");"
        // insert into guess_server_record_flow(
        // `event_time`,`room_type`,`room_id`,`stage`,`red_cards`,`blue_cards`,`red_result`,`blue_result`
        // ) values (
        // '1586844573','1','131','1586844573','[6,7,8,8,9]','[1,1,1,1,10]','-4','4');
        try {
          str = URLDecoder.decode(str, "utf-8")
        } catch {
          case e => e.printStackTrace()
        }
        str
      }
    sqldata
  }
}
