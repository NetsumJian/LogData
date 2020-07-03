package com.haofei.service

import org.apache.spark.sql.SparkSession

object TestDriver02 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("logistic")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext
    sc.makeRDD(Array("a","b","c","d","e")).foreach(println)
  }
}
