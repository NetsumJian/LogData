package com.haofei.service

import java.util.Calendar

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[5]").setAppName("kafaksource")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))
    val zkHosts = "test01:2181"
    val groupId = "kafka1910"
    val topics = Map("weblog"-> 2)
    val kafkaSource = KafkaUtils.createStream(ssc,zkHosts,groupId,topics).map(_._2)
    kafkaSource.print()
    // http://localhost:8080/FluxAppServer/a.jsp|a.jsp|页面A|UTF-8
    // |1440x900|24-bit|zh-cn|0|1||0.958978394097983|
    // |Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36|01343585556836203539
    // |6695634189_0_1586748864183|127.0.0.1
    /*kafkaSource.foreachRDD{x =>
      val lines = x.toLocalIterator
      while (lines.hasNext) {
        val line = lines.next()
        val info = line.split("\\|")
        val url = info(0)
        val urlname = info(1)
        val uvid = info(13)
        val ssid = info(14).split("_")(0)
        val sscount = info(14).split("_")(1)
        val sstime = info(14).split("_")(2)
        val cip = info(15)
        val logBean = LogBean(url,urlname,uvid,ssid,sscount,sstime,cip)
        // println(logBean)
        val pv = 1
        val calendar = Calendar.getInstance()
        val endTime = sstime.toLong
        calendar.setTimeInMillis(endTime)
        calendar.set(Calendar.HOUR,0)
        calendar.set(Calendar.MINUTE,0)
        calendar.set(Calendar.SECOND,0)
        calendar.set(Calendar.MILLISECOND,0)
        val startTime = calendar.getTimeInMillis

        val uvRegex="^\\d+_"+uvid+".*$"
        val uvResultRDD = HBaseUtil.queryByRangeAndRegex(sc, startTime, endTime, uvRegex)
        val uv = if(uvResultRDD.count() == 0) 1 else 0
        // println("pv:" + pv + "uv:"+uv)

        val vvRegex = "^\\d+_\\d+_"+ssid+".*$"
        val vvResultRDD = HBaseUtil.queryByRangeAndRegex(sc,startTime,endTime,vvRegex)
        val vv = if(vvResultRDD.count() == 0) 1 else 0
        // println("pv:" + pv + " uv:"+ uv + " vv: "+vv)

        val newipRegex = "^\\d+_\\d+_\\d+_"+cip + ".*$"
        val newipResultRDD = HBaseUtil.queryByRangeAndRegex(sc,0,endTime,newipRegex)
        val newip = if(newipResultRDD.count() == 0 ) 1 else 0

        val newcustRegex = "^\\d+_"+uvid+".*$"
        val newcastResultRDD = HBaseUtil.queryByRangeAndRegex(sc,0,endTime,newcustRegex)
        val newcust = if (newcastResultRDD.count() == 0) 1 else 0
        val tongjiBean = TongjiBean(sstime,pv,uv,vv,newip,newcust)
        println(tongjiBean)
        MysqlUtil.saveToMysql(tongjiBean)
        HBaseUtil.saveToHBase(sc,logBean)
      }
    }*/

    ssc.start()
    ssc.awaitTermination()
  }

}
