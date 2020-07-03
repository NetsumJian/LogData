package com.haofei.service

import java.net.URI

import com.haofei.utils.TslogUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

object TslogDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("tslog")
    val sc = new SparkContext(conf)

    val yesterday = new DateTime().toLocalDate.toString()
    println(yesterday)
    val rdb = "data"
    val config = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://haofei1:9000"),config,"root")

    val paths = TslogUtil.getPathList(fs,rdb,yesterday)

    for (i <- 0 until paths.size) {
      val data = sc.textFile(paths(i))
      val parsedata = TslogUtil.parseData(data)
      parsedata.foreach{case (name ,data) =>
        TslogUtil.saveToHdfs(data,rdb,name,i,yesterday)
      }
    }

  }
}
