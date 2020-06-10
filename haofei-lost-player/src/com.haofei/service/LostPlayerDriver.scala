package com.haofei.service

import com.haofei.util.{EmailUtil, MysqlUtil, SqlUtil}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._
import com.haofei.domain.{ReaderDataSource, SXGFDataSource}

object LostPlayerDriver {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory","16g")
      .appName("LostPlayer")
      .getOrCreate()

    val today = new DateTime().toLocalDate
    val traintSql = SqlUtil.getTraintSql(today-30.day)

    val traintArray = MysqlUtil.getTraintArray(traintSql,ReaderDataSource)

    val traintDF = spark.createDataFrame(traintArray).toDF("id","vip","factor","nums","atime","la")

    val vectorDf = new VectorAssembler().setInputCols(Array("nums","atime")).setOutputCol("features").transform(traintDF)

    val indexDf = new StringIndexer().setInputCol("la").setOutputCol("label").fit(vectorDf).transform(vectorDf)

    val logisticModel = new LogisticRegression()
      .setMaxIter(20)
      .setWeightCol("nums")
      .fit(indexDf)

    val testSql = SqlUtil.getPredictSql(today-1.day)

    val testArray = MysqlUtil.getTestArray(testSql,ReaderDataSource)

    val testDF = spark.createDataFrame(testArray).toDF("id","vip","factor","nums","atime")
    val testVector = new VectorAssembler().setInputCols(Array("nums","atime")).setOutputCol("features").transform(testDF)
    val predictDF = logisticModel.transform(testVector)

    val sqls = predictDF.where("prediction=1").rdd.map{row =>
      val sql = s"insert into data_bi.lost_player values(null,'${System.currentTimeMillis()/1000}','$today','${row(0)}','${row(1)}','${row(2)}','${row(3)}','${row(4)}','${row(7)}','${row(8)}')"
      sql
    }.collect()

    MysqlUtil.saveSingleTable(sqls,SXGFDataSource)
    EmailUtil.sendSimpleTextEmail("Spark定时任务执行报告",s"$today,流失玩家模型执行成功")
  }
}
