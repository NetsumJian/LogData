package com.haofei.service


import java.net.URLDecoder
import java.text.SimpleDateFormat

import com.haofei.utils.MysqlUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object MysqlDriver {
  def main(args: Array[String]): Unit = {
    /*val conf = new SparkConf()
      .setMaster("spark://hadoop3:7077")
      .setAppName("HaoFeiLog")
      .set("spark.streaming.concurrentJobs", "4")
      .set("spark.executor.memory", "4g")*/
    val conf = new SparkConf()
      .setMaster("local[8]")
      .set("spark.streaming.concurrentJobs", "4")
      .setAppName("localtest")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(30))
    val zkHosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181"
    val sxgfGroup = "sxgf"
    val sxgfTopic = Map("data_tslog" -> 2)
    val sxgfSource = KafkaUtils.createStream(ssc, zkHosts, sxgfGroup, sxgfTopic).map(_._2)


    sxgfSource.foreachRDD{rdd =>
      println("-------------sxgf------------------")
      rdd.take(3).foreach(println)
    }

    val sxqdGroup = "sxqd"
    val sxqdTopic = Map("weblog" -> 2)
    val sxqdSource = KafkaUtils.createStream(ssc, zkHosts, sxqdGroup, sxqdTopic).map(_._2)

    sxqdSource.foreachRDD{rdd =>
      println("-------------sxqd------------------")
      rdd.take(3).foreach(println)
    }
    /*
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
          println("总计:"+sqldata.count)
        }
    */

    ssc.start()
    ssc.awaitTermination()

  }
}
