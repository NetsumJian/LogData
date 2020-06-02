package com.haofei.service

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession

object TslogDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("tslogApp").getOrCreate
    val sc = spark.sparkContext

    val yesterday = new SimpleDateFormat("yyyy-MM-dd")
      .format(new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000))
    // val yesterday = "2020-04-30"

    val path = "hdfs://hadoop1:9000/data_tslog/reportTime=" + yesterday
    val data = sc.wholeTextFiles(path).map(_._2).flatMap(_.split("\n"))

    val parsedata = data.filter(_.matches("\\w+\\|.*"))
      .map { line =>
        val fileName = line.substring(0, line.indexOf("|"))
        val lineData = line.substring(line.indexOf("|") + 1)
        (fileName, lineData)
      }
      .groupByKey

    val fileNames = parsedata.map(_._1).collect
    val fileDatas = parsedata.map(_._2.toArray).collect

    for (i <- 0 until fileNames.size) {
      sc.makeRDD(fileDatas(i)).saveAsTextFile("hdfs://hadoop1:9000/hive_tslog/"
        + fileNames(i) + "/reportTime=" + yesterday)
      println(System.currentTimeMillis() / 1000 + "|" + fileNames(i) + "|" + fileDatas(i).size + "|" + 1)
    }

  }
}
