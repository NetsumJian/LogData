package com.haofei.service

import java.text.SimpleDateFormat

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SqoopDriver {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("sqooplog")
    val sc = new SparkContext(conf)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val data = sc.wholeTextFiles("hdfs://test01:9000/user/root/battery_use_skill_flow/*")
    data.persist(StorageLevel.DISK_ONLY)
    val filterdata = data.map(_._2).flatMap(_.split("\\s+")).map{line =>
      val ts = line.substring(0,line.indexOf(","))
      val ms = line.substring(line.indexOf(",")+1,line.lastIndexOf(","))
      val time = sdf.format(ts.toLong)
      (time,ms)
    }
    filterdata.foreach(println)
    println(filterdata.count)

  }

}
