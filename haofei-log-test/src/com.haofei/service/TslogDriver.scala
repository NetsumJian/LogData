package com.haofei.service

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object TslogDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("spark://hadoop3:7077")
      .setAppName("tslog")
      .set("spark.executor.memory", "4g")
    // val conf = new SparkConf().setMaster("local").setAppName("tslog")
    val sc = new SparkContext(conf)

    val yesterday = new SimpleDateFormat("yyyy-MM-dd")
      .format(new Date(System.currentTimeMillis() - 24*60*60*1000))

    // val path = "D:\\Document\\TestData\\part-0"
    val path = "hdfs://hadoop1:9000/har_tslog/reportTime="+yesterday+".har/part-0"
    val data = sc.textFile(path,10)

    val parsedata = data.filter( ! _.contains("\\|") )
      .map{ line =>
        val fileName = line.substring(0,line.indexOf("|"))
        val lineData = line.substring(line.indexOf("|")+1)
        (fileName,lineData)
      }
      .groupByKey

    val fileNames = parsedata.map(_._1).collect
    val fileDatas = parsedata.map(_._2.toArray).collect

    for (i <- 0 until fileNames.size){
      sc.makeRDD(fileDatas(i)).saveAsTextFile("hdfs://hadoop1:9000/hive_tslog/"
        +fileNames(i)+"/reportTime="+yesterday )
      println(System.currentTimeMillis()/1000+"|"+fileNames(i)+"|"+fileDatas(i).size+"|"+1)
    }

  }
}
