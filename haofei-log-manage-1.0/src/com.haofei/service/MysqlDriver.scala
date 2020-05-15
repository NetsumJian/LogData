package com.haofei.service


import java.net.URLDecoder

import com.haofei.util.MysqlUtil
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object MysqlDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("spark://hadoop3:7077")
      .setAppName("HaoFeiLog")
      .set("spark.executor.memory", "4g")
    // val conf = new SparkConf().setMaster("local[5]").setAppName("localtest")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(60))
    val zkHosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181"
    val groupId = "hfmysql"
    val topics = Map("data_tslog" -> 2)
    val kafkaSource = KafkaUtils.createStream(ssc, zkHosts, groupId, topics).map(_._2)

    val dataBase = "data_tslog"
    val tableMap = MysqlUtil.getTableMap(dataBase)

    kafkaSource.foreachRDD { rdd =>
      // 数据过滤解析 -> 去除异常数据
      val parsedata = rdd.filter(_.matches("\\w+\\|.*"))
        .map(_.split("\\|"))
        .sortBy(_ (0))
      // 数据处理 Array[String] -> sql语句
      val sqldata = parsedata
        .filter(a => tableMap.contains(a(0)))
        .map { arr =>
          val tableName = arr(0)
          val columnArray = tableMap(tableName)
          var str = ""
          for (i <- 0 until arr.size) {
            if (i == 0) {
              // insert into guess_server_record_flow(
              str = "insert into " + tableName + "("
              for (j <- 0 until columnArray.size){
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
          URLDecoder.decode(str,"utf-8")
      }
      MysqlUtil.saveToMysql(sqldata.collect)
      // println("总计:"+sqldata.count)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
