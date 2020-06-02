package com.haofei.service

import java.text.SimpleDateFormat

import com.haofei.domain.{LocalDataSource, SXGFDataSource, SXQDDataSource, TTGFDataSource, TTQDDataSource, TestDataSource}
import com.haofei.utils.{MysqlUtil2, RddUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MysqlDriver2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("mysqlApp")
      .config("spark.streaming.concurrentJobs", "2")
      .getOrCreate
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(30))

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val zkHosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181"

    // 手心渠道配置及业务逻辑处理
    val sxqdTopic = Map("sxqd_tslog" -> 3)
    val sxqdSource = KafkaUtils.createStream(ssc, zkHosts, "test", sxqdTopic).map(_._2)

    val sxqdMap = MysqlUtil2.getTableMap("data_tslog",TestDataSource)

    sxqdSource.foreachRDD { rdd =>
      val sqldata = RddUtil.rddToSql(rdd,sxqdMap)
      MysqlUtil2.saveToMysql(sqldata.collect,TestDataSource)
      println(sdf.format(System.currentTimeMillis)+"|sxqd_tslog|"+sqldata.count)
    }

    // 手心官方配置及业务逻辑处理
    val sxgfTopic = Map("sxgf_tslog" -> 3)
    val sxgfSource = KafkaUtils.createStream(ssc, zkHosts, "local", sxgfTopic).map(_._2)

    val sxgfMap = MysqlUtil2.getTableMap("data_tslog",LocalDataSource)

    sxgfSource.foreachRDD { rdd =>
      val sqldata = RddUtil.rddToSql(rdd,sxgfMap)
      MysqlUtil2.saveToMysql(sqldata.collect,LocalDataSource)
      println(sdf.format(System.currentTimeMillis)+"|sxgf_tslog|"+sqldata.count)
    }
/*

    // 天天渠道配置及业务逻辑处理
    val ttqdTopic = Map("ttqd_tslog" -> 3)
    val ttqdSource = KafkaUtils.createStream(ssc, zkHosts, "ttqd", ttqdTopic).map(_._2)

    val ttqdMap = MysqlUtil2.getTableMap("ttqd_tslog",SXQDDataSource)

    ttqdSource.foreachRDD { rdd =>
      val sqldata = RddUtil.rddToSql(rdd,ttqdMap)
      MysqlUtil2.saveToMysql(sqldata.collect,TTQDDataSource)
      println(sdf.format(System.currentTimeMillis)+"|ttqd_tslog|"+sqldata.count)
    }

    // 天天官方配置及业务逻辑处理
    val ttgfTopic = Map("ttgf_tslog" -> 3)
    val ttgfSource = KafkaUtils.createStream(ssc, zkHosts, "ttgf", ttgfTopic).map(_._2)

    val ttgfMap = MysqlUtil2.getTableMap("ttgf_tslog",SXQDDataSource)

    ttgfSource.foreachRDD { rdd =>
      val sqldata = RddUtil.rddToSql(rdd,ttgfMap)
      MysqlUtil2.saveToMysql(sqldata.collect,TTGFDataSource)
      println(sdf.format(System.currentTimeMillis)+"|ttgf_tslog|"+sqldata.count)
    }
*/

    // 启动实时流
    ssc.start()
    ssc.awaitTermination()

  }
}
