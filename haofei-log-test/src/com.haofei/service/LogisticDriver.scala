package com.haofei.service

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature. VectorAssembler
import org.apache.spark.sql.SparkSession

object LogisticDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("logistic")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext
    val sqc = spark.sqlContext

    val traintData = sc.textFile("E:\\datawork\\ttqd_traint_0602.csv")
    val parseData = traintData.map{line =>
      val arr = line.split(",")
      val id = arr(0)
      val vip = arr(1).toDouble
      val factor = arr(2).toDouble
      val cost = arr(3).toDouble
      val days = arr(4).toDouble
//      val days = arr(3).toDouble
//      val onlineTime = arr(4).toDouble
      val onlineTime = arr(5).toDouble
      val fireTimes = arr(6).toDouble
      val la = arr(7).toDouble
//      val la = arr(5).toDouble
      (id,vip,factor,cost,days,onlineTime,fireTimes,la)
//      (id,vip,factor,days,onlineTime,la)
    }
    val df = sqc.createDataFrame(parseData)
      .toDF("id","vip","factor","cost","days","onlineTime","fireTimes","la")
//      .toDF("id","vip","factor","days","onlineTime","la")

    val vectorDf = new VectorAssembler()
      .setInputCols(Array("days","onlineTime","fireTimes"))
      .setOutputCol("features").transform(df)

    // val indexDf = new StringIndexer().setInputCol("la").setOutputCol("label").fit(vectorDf).transform(vectorDf)
    // indexDf.show()

    // LogisticRegression
    val model = new LogisticRegression()
      .setLabelCol("la")
      .setMaxIter(20)
      .setWeightCol("days")
      .fit(vectorDf)

    val checkData = model.transform(vectorDf)
    checkData.show()
/*    for ( i <- 0 until 10 ){
      val p = checkData.select("id","la").where(s"prediction=1 and vip=$i").count()
      val lp = checkData.select("id","la").where(s"prediction=1 and la=1 and vip=$i").count()
      val l = checkData.select("id","la").where(s"la=1 and vip=$i").count()
      val v = checkData.select("id","vip").where(s"vip=$i").count()
      println(s"预流失匹配实际流失人数lp$i : $lp \t 预流失总人数p$i : $p \t 实际流失总人数l$i : $l \t 活跃玩家总人数v$i : $v \t 预流失准确率lp/p : " + (lp.toDouble/p.toDouble).formatted("%.4f")
        +" \t 流失准确率lp/l : " + (lp.toDouble/l.toDouble).formatted("%.4f") + " \t 预流失率p/v : " + (p.toDouble/v.toDouble).formatted("%.4f")
      +" \t 实际流失率l/v : " + (l.toDouble/v.toDouble).formatted("%.4f"))
    }*/
    checkData.take(20).foreach{row =>
      println(row(10))
      /*val pro = row(10).toString.dropRight(1).drop(1).split(',')
      val d = pro(1).toDouble - pro(0).toDouble
      println(d.formatted("%.6f"))*/
    }
/*
    checkData.select("id").where("prediction=1 and vip=6 ")
      .toJavaRDD.saveAsTextFile("E:\\datawork\\result")

    val predictData = sc.textFile("E:\\datawork\\ttqd_pred_0615.csv")
    val ppd = predictData.map{line =>
      val arr = line.split(",")
      val id = arr(0)
      val vip = arr(1).toDouble
      val factor = arr(2).toDouble
      val days = arr(3).toDouble
      val atime = arr(4).toDouble
      (id,vip,factor,days,atime)
    }
    val pdf = sqc.createDataFrame(ppd).toDF("id","vip","factor","days","atime")
    val pvdf = new VectorAssembler().setInputCols(Array("days","atime")).setOutputCol("features").transform(pdf)
    val predictions = model.transform(pvdf)

    predictions.select("id").where("prediction=1 and vip=6 ")
      .toJavaRDD.saveAsTextFile("E:\\datawork\\predict")
    */
  }
}
