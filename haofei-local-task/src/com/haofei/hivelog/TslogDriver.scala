package com.haofei.hivelog

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

    val path = "hdfs://hadoop1:9000/sxqd_tslog/reportTime=" + yesterday
    val data = sc.wholeTextFiles(path,20).map(_._2).flatMap(_.split("\n"))

    val parsedata = data
      .filter(_.matches("^([a-z]+_)+[a-z]+\\|.*"))
      .map { line =>
        val fileName = line.substring(0, line.indexOf("|"))
        val lineData = line.substring(line.indexOf("|") + 1)
        (fileName, lineData)
      }
      .groupByKey
    parsedata.persist()

    val fileNames = parsedata.map(_._1).collect
    val fileDatas = parsedata.map(_._2.toArray).collect

    for (i <- 0 until fileNames.size) {
      sc.makeRDD(fileDatas(i)).saveAsTextFile("hdfs://hadoop1:9000/hive_tslog/sxqd/"
        + fileNames(i) + "/report_time=" + yesterday)
      println(System.currentTimeMillis() / 1000 + "|" + fileNames(i) + "|" + fileDatas(i).size + "|" + 1)
    }

  }
}
