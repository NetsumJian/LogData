package com.haofei.service

import com.haofei.domain.TestDataSource
import com.haofei.utils.{MysqlUtil, RddUtil}
import org.apache.spark.sql.SparkSession

object InsertDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("inLog")
      .getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile("E:\\datawork\\testlog.txt")

    val tableMap = MysqlUtil.getTableMap("data_tslog",TestDataSource)
    val sqlArray = RddUtil.rddToSql(data,tableMap)
    sqlArray.foreach(println)
  }
}
