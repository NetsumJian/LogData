package com.haofei.service

import java.net.URLDecoder
import java.text.SimpleDateFormat

import com.haofei.utils.{AuthDataSource, C3P0Util, MysqlUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object JdbcDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("tslog")
    val sc = new SparkContext(conf)
    // val nowday = new SimpleDateFormat("yyyy-MM-dd").format(System.currentTimeMillis())

    val path = "D:\\Document\\TestData\\tslog_9.20200414.01"
    val dataBase = "data_tslog"
    val map = MysqlUtil.getTableMap(dataBase)

    val data = sc.textFile(path)
    // data.saveAsTextFile("hdfs://test01:9000/data_tslog/reporttime="+nowday+"/"+System.currentTimeMillis())
    // data.foreach(println)

    val parsedata = data.map(_.split("\\|")).sortBy(_ (0))
    val sqldata = parsedata
      .filter(a => map.contains(a(0)))
      .map { arr =>
        val tableName = arr(0)
        val columnArray = map(tableName)
        var str = ""
        for (i <- 0 until arr.size) {
          if (i == 0) {
            str = "insert into " + tableName + "("
            for (j <- 0 until columnArray.size){
              str = str + "`" + columnArray(j) + "`,"
            }
            str = str.dropRight(1) + ") values ("
          }
          else str = str + "'" + arr(i) + "',"
        }
        str = str.dropRight(1) + ");"
        URLDecoder.decode(str,"utf-8")
      }
    // C3P0Util.saveToMysql(sqldata.collect())
    sqldata.foreach(println)
    // println(data.count())
  }
}
