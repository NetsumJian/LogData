package com.haofei.service

import com.haofei.domain.{SXGFDataSource, SXQDDataSource, TTGFDataSource, TTQDDataSource}
import com.haofei.util.{EmailUtil, JdbcUtil, RddUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object MysqlDriver {

  val logger = LoggerFactory.getLogger(MysqlDriver.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("mysqlApp")
      .config("spark.streaming.concurrentJobs", "16")
      .getOrCreate
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))


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
      if ( (System.currentTimeMillis/1000-57600) % 86400 < 5 ){
        sxgfMap = JdbcUtil.getTableMap("sxgf_tslog",dsValue(0))
      }
      val sqldata = RddUtil.rddToSql(rdd, sxgfMap)
      sqldata.persist()
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(0))
      logger.warn(s"sxgf_tslog|${sqldata.count}")
      sqldata.unpersist()
    }

    // 手心渠道业务逻辑处理
    sxqdSource.foreachRDD { rdd =>
      if ( (System.currentTimeMillis/1000-57600) % 86400 < 5 ){
        sxqdMap = JdbcUtil.getTableMap("sxqd_tslog",dsValue(1))
      }
      val sqldata = RddUtil.rddToSql(rdd, sxqdMap)
      sqldata.persist()
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(1))
      logger.warn(s"sxqd_tslog|${sqldata.count}")
      sqldata.unpersist()
    }

    // 天天官方业务逻辑处理
    ttgfSource.foreachRDD { rdd =>
      if ( (System.currentTimeMillis/1000-57600) % 86400 < 5 ){
        ttgfMap = JdbcUtil.getTableMap("ttgf_tslog",dsValue(2))
      }
      val sqldata = RddUtil.rddToSql(rdd, ttgfMap)
      sqldata.persist()
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(2))
      logger.warn(s"ttgf_tslog|${sqldata.count}")
      sqldata.unpersist()
    }

    // 天天渠道业务逻辑处理
    ttqdSource.foreachRDD { rdd =>
      if ( (System.currentTimeMillis/1000-57600) % 86400 < 5 ){
        ttqdMap = JdbcUtil.getTableMap("ttqd_tslog",dsValue(3))
      }
      val sqldata = RddUtil.rddToSql(rdd, ttqdMap)
      sqldata.persist()
      JdbcUtil.saveToMysql(sqldata.collect, dsValue(3))
      logger.warn(s"ttqd_tslog|${sqldata.count}")
      sqldata.unpersist()
    }

    // 启动实时流
    ssc.start()
    ssc.awaitTermination()

  }
}
