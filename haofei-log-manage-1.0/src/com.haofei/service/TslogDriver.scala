package com.haofei.service

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SparkSession

object TslogDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[8]")
      .config("spark.executor.memory", "8g")
      .appName("tslog").getOrCreate
    val sc = spark.sparkContext

    val yesterday = new SimpleDateFormat("yyyy-MM-dd")
      .format(new Date(System.currentTimeMillis() - 24 * 60 * 60 * 1000))

    // val path = "D:\\Document\\TestData\\part-0"
    val path = "hdfs://hadoop1:9000/har_tslog/reportTime=" + yesterday + ".har/part-0"
    val data = sc.textFile(path, 10)

    val parsedata = data.filter(!_.contains("\\|"))
      .map { line =>
        val fileName = line.substring(0, line.indexOf("\\|"))
        val lineData = line.substring(line.indexOf("\\|") + 1)
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
