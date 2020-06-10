package com.haofei.service

import java.text.SimpleDateFormat

import com.haofei.domain.{SXGFDataSource, SXQDDataSource, TTGFDataSource, TTQDDataSource}
import com.haofei.util.{EmailUtil, JdbcUtil, RddUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MysqlDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("mysqlApp")
      .config("spark.streaming.concurrentJobs", "4")
      .getOrCreate
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(30))

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val zkHosts = "hadoop1:2181,hadoop2:2181,hadoop3:2181"

    // 设置 DataSource 广播变量
    val dsValue = sc.broadcast(Array(SXGFDataSource,SXQDDataSource,TTGFDataSource,TTQDDataSource)).value

    // 手心官方配置
    val sxgfTopic = Map("sxgf_tslog" -> 3)
    val sxgfSource = KafkaUtils.createStream(ssc, zkHosts, "sxgf", sxgfTopic).map(_._2)
    var sxgfMap = JdbcUtil.getTableMap("sxgf_tslog",dsValue(0))

    // 手心渠道配置
    val sxqdTopic = Map("sxqd_tslog" -> 3)
    val sxqdSource = KafkaUtils.createStream(ssc, zkHosts, "sxqd", sxqdTopic).map(_._2)
    var sxqdMap = JdbcUtil.getTableMap("sxqd_tslog",dsValue(1))

    // 天天官方配置
    val ttgfTopic = Map("ttgf_tslog" -> 3)
    val ttgfSource = KafkaUtils.createStream(ssc, zkHosts, "ttgf", ttgfTopic).map(_._2)
    var ttgfMap = JdbcUtil.getTableMap("ttgf_tslog",dsValue(2))

    // 天天渠道配置
    val ttqdTopic = Map("ttqd_tslog" -> 3)
    val ttqdSource = KafkaUtils.createStream(ssc, zkHosts, "ttqd", ttqdTopic).map(_._2)
    var ttqdMap = JdbcUtil.getTableMap("ttqd_tslog",dsValue(3))

    // 设置 tableMap 广播变量
    // val mapValue = sc.broadcast(Array(sxgfMap,sxqdMap,ttgfMap,ttqdMap)).value

    // 手心官方业务逻辑处理
    sxgfSource.foreachRDD { rdd =>
      val t = (System.currentTimeMillis/1000) % 86400
      if ( t < 60 ){
        sxgfMap = JdbcUtil.getTableMap("sxgf_tslog",dsValue(0))
        EmailUtil.sendSimpleTextEmail("手心官方表结构重新加载",s"${sxgfMap.mkString("\n")}")
      }
      val sqldata = RddUtil.rddToSql(rdd, sxgfMap)
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(0))
      println(sdf.format(System.currentTimeMillis) + "|sxgf_tslog|" + sqldata.count)
    }

    // 手心渠道业务逻辑处理
    sxqdSource.foreachRDD { rdd =>
      val t = (System.currentTimeMillis/1000) % 86400
      if ( t < 60 ){
        sxqdMap = JdbcUtil.getTableMap("sxqd_tslog",dsValue(0))
        EmailUtil.sendSimpleTextEmail("手心渠道表结构重新加载",s"${sxqdMap.mkString("\n")}")
      }
      val sqldata = RddUtil.rddToSql(rdd, sxqdMap)
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(1))
      println(sdf.format(System.currentTimeMillis) + "|sxqd_tslog|" + sqldata.count)
    }

    // 天天官方业务逻辑处理
    ttgfSource.foreachRDD { rdd =>
      val t = (System.currentTimeMillis/1000) % 86400
      if ( t < 60 ){
        ttgfMap = JdbcUtil.getTableMap("ttgf_tslog",dsValue(0))
        EmailUtil.sendSimpleTextEmail("天天官方表结构重新加载",s"${ttgfMap.mkString("\n")}")
      }
      val sqldata = RddUtil.rddToSql(rdd, ttgfMap)
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(2))
      println(sdf.format(System.currentTimeMillis) + "|ttgf_tslog|" + sqldata.count)
    }

    // 天天渠道业务逻辑处理
    ttqdSource.foreachRDD { rdd =>
      val t = (System.currentTimeMillis/1000) % 86400
      if ( t < 60 ){
        ttqdMap = JdbcUtil.getTableMap("ttqd_tslog",dsValue(0))
        EmailUtil.sendSimpleTextEmail("天天渠道表结构重新加载",s"${ttqdMap.mkString("\n")}")
      }
      val sqldata = RddUtil.rddToSql(rdd, ttqdMap)
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(3))
      println(sdf.format(System.currentTimeMillis) + "|ttqd_tslog|" + sqldata.count)
    }

    // 启动实时流
    ssc.start()
    ssc.awaitTermination()

  }
}
