package com.haofei.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object TslogUtil {

  def parseData (data:RDD[String]):RDD[(String, Iterable[String])]= {
    data.filter(_.matches("^([a-z]+_)+[a-z]+\\|.*"))
      .map { line =>
        val fileName = line.substring(0, line.indexOf("|"))
        val lineData = line.substring(line.indexOf("|") + 1)
        (fileName, lineData)
      }
      .groupByKey
  }

  def hadleTslog(rdb:String,part:Int,sc:SparkContext,yesterday:String) = {
    val path = s"hdfs://hadoop1:9000/${rdb}_tslog/reportTime=$yesterday"
    val ds = sc.wholeTextFiles(path,part).map(_._2).flatMap(_.split("\n"))

    val parsedata = parseData(ds)

    parsedata.persist()

    val fileNames = parsedata.map(_._1).collect
    val fileDatas = parsedata.map(_._2.toArray).collect

    parsedata.foreach{case (name,data) =>
      sc.makeRDD(data.toList).saveAsTextFile(s"hdfs://hadoop1:9000/hive_tslog/$rdb/$name/report_time=$yesterday")
    }
    for (i <- 0 until fileNames.size) {
      sc.makeRDD(fileDatas(i)).saveAsTextFile(s"hdfs://hadoop1:9000/hive_tslog/$rdb/${fileNames(i)}/report_time=$yesterday")
    }
    parsedata.unpersist()
  }

  def saveToHdfs(it: Iterable[String],rdb:String,table:String,part:Int,date:String ): Unit ={
    import java.io.ByteArrayInputStream
    val config = new Configuration()
    val fs = FileSystem.get(new URI("hdfs://haofei1:9000"),config,"root")
    val out = fs.create(new Path(s"/hive_tslog/$rdb/$table/reportTime=$date/part-$part"))
    val in = new ByteArrayInputStream(it.mkString("\n").getBytes)
    IOUtils.copyBytes(in, out, fs.getConf)
    IOUtils.closeStream(in)
  }

  def getPathList(fs:FileSystem,rdb:String,date:String):Array[String]  ={
    val filefileStatuses = fs.listStatus(new Path(s"/${rdb}_tslog/reportTime=$date"))
    filefileStatuses.map{status =>
      status.getPath.toString
    }
  }
}
