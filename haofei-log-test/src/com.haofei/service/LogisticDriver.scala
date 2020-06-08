package com.haofei.service

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.SparkSession

object LogisticDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("logistic")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext
    val sqc = spark.sqlContext

    val traintData = sc.textFile("E:\\datawork\\traint.csv")
    val parseData = traintData.map{line =>
      val arr = line.split(",")
      val id = arr(0)
      val vip = arr(1).toDouble
      val factor = arr(2).toDouble
      val nums = arr(3).toDouble
      val time = arr(4).toDouble/60
      val la = arr(5).toDouble
      (id,vip,factor,nums,time,la)
    }
    val df = sqc.createDataFrame(parseData).toDF("id","vip","factor","nums","time","la")
    // df.show()

    val vectorDf = new VectorAssembler().setInputCols(Array("nums","time")).setOutputCol("features").transform(df)
    // vectorDf.show()

    val indexDf = new StringIndexer().setInputCol("la").setOutputCol("label").fit(vectorDf).transform(vectorDf)
    // indexDf.show()

    val logisticModel = new LogisticRegression()
      .setMaxIter(20)
      .setWeightCol("nums")
      .fit(indexDf)

    val checkData = logisticModel.transform(indexDf)
//    checkData.toJavaRDD.saveAsTextFile("E:\\datawork\\prediction")
    checkData.show()
    for ( i <- 0 until 10 ){
      val p = checkData.select("id","la").where(s"prediction=1 and vip=$i").count()
      val lp = checkData.select("id","la").where(s"prediction=1 and la=1 and vip=$i").count()
      val l = checkData.select("id","la").where(s"la=1 and vip=$i").count()
      val v = checkData.select("id","vip").where(s"vip=$i").count()
      println(s"准确预测人数lp$i : $lp \t 预测总人数p$i : $p \t 实际流失人数l$i : $l \t 活跃玩家人数v$i : $v \t 预测准确率lp/p : " + (lp.toDouble/p.toDouble).formatted("%.4f")
        +" \t 流失预测准确率lp/l : " + (lp.toDouble/l.toDouble).formatted("%.4f") + " \t 预测率p/v : " + (p.toDouble/v.toDouble).formatted("%.4f")
      +" \t 玩家流失率l/v : " + (l.toDouble/v.toDouble).formatted("%.4f"))
    }

    /*val predictData = sc.textFile("E:\\datawork\\predicate.txt")
    val ppd = predictData.map{line =>
      val arr = line.split(",")
      val id = arr(0)
      val vip = arr(1).toDouble
      val factor = arr(2).toDouble
      val nums = arr(3).toDouble
      val time = arr(4).toDouble
      (id,vip,factor,nums,time)
    }
    val pdf = sqc.createDataFrame(ppd).toDF("id","vip","factor","nums","time")
    val pvdf = new VectorAssembler().setInputCols(Array("nums","time")).setOutputCol("features").transform(pdf)
    val predictions = logisticModel.transform(pvdf)

    val javaRDD = predictions.select("id","vip","factor","nums","time").where("prediction=1").sort("vip").toJavaRDD
    javaRDD.saveAsTextFile("E:\\datawork\\result")
    println(s"${javaRDD.count()}")*/
  }
}
