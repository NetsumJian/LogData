package com.haofei.machine

import java.text.SimpleDateFormat

import com.haofei.domain.{LocalDataSource, TestDataSource}
import com.haofei.utils.SqlUtil
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object LostPlayerDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("LostPlayer")
      .getOrCreate()

    val conn = TestDataSource.getConnection()
    val traintSql = SqlUtil.getTraintSql()
    val ps = conn.prepareStatement(traintSql)
    val traintRS = ps.executeQuery()
    val traintArray = new mutable.ArrayBuffer[(Int,Int,Int,Double,Double,Double)]()
    while (traintRS.next()){
      val id = traintRS.getInt("id")
      val vip = traintRS.getInt("vip")
      val factor = traintRS.getInt("factor")
      val nums = traintRS.getInt("nums")
      val atime = traintRS.getDouble("atime")
      val la = traintRS.getInt("la")
      traintArray.append((id,vip,factor,nums,atime,la))
    }
    val traintDF = spark.createDataFrame(traintArray).toDF("id","vip","factor","nums","atime","la")

    val vectorDf = new VectorAssembler().setInputCols(Array("nums","atime")).setOutputCol("features").transform(traintDF)

    val indexDf = new StringIndexer().setInputCol("la").setOutputCol("label").fit(vectorDf).transform(vectorDf)

    val logisticModel = new LogisticRegression()
      .setMaxIter(20)
      .setWeightCol("nums")
      .fit(indexDf)

    val testSql = SqlUtil.getPredictSql()
    val testRS = ps.executeQuery(testSql)
    val testArray = new mutable.ArrayBuffer[(Int,Int,Int,Double,Double)]()
    while (testRS.next()){
      val id = testRS.getInt("id")
      val vip = testRS.getInt("vip")
      val factor = testRS.getInt("factor")
      val nums = testRS.getInt("nums")
      val atime = testRS.getDouble("atime")
      testArray.append((id,vip,factor,nums,atime))
    }
    val testDF = spark.createDataFrame(testArray).toDF("id","vip","factor","nums","atime")
    val testVector = new VectorAssembler().setInputCols(Array("nums","atime")).setOutputCol("features").transform(testDF)
    val predictDF = logisticModel.transform(testVector)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.format(System.currentTimeMillis())
    predictDF.take(10).foreach{row =>
      val sql = s"insert into lostplayer values(null,'${row(0)}','${System.currentTimeMillis()/1000}','$today','${row(1)}','${row(2)}','${row(3)}','${row(4)}','${row(8)}')"
      println(sql)
      ps.execute(sql)
    }
  }
}
