package com.haofei.service

import org.apache.spark.sql.SparkSession

object HiveDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[8]")
      .config("spark.executor.memory", "8g")
      .appName("hivelog").getOrCreate
    val sc = spark.sparkContext

    val data = sc.wholeTextFiles("hdfs://test01:9000/weblog/reporttime=2020-05-13/",10)
    val parseData = data.map(_._2).flatMap(_.split("\n"))
    parseData.filter( _.contains("|")).take(100).foreach(println)
  }
}
