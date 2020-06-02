package com.haofei.service

import java.text.SimpleDateFormat

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object KmeanDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("traffic-kmeans")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val nowTime = sdf.format(System.currentTimeMillis())
    val dataPath = "D:\\Document\\TestData\\active_player2020-05-25.txt"
    val userData = sc.textFile(dataPath)
    // userData.take(20).foreach(println)
    val parseData = userData.map(_.split("\\|"))
      .map{ x =>
      val id = x(0).toDouble
      val registerTime = x(1).toDouble
      val payAmount = x(2).toDouble
      val factor = x(3).toDouble
      val onlineTime = x(4).toDouble
      (id,registerTime,payAmount,factor,onlineTime)
    }
    // parseData.take(10).foreach(println)
    val userDataFrame = sqc.createDataFrame(parseData).toDF("id","register","cost","factor","online")
    // userDataFrame.show()
    val va = new VectorAssembler().setInputCols(Array("factor","online")).setOutputCol("features")
    val userVector = va.transform(userDataFrame)
    val userModel = new KMeans().setK(30).setFeaturesCol("features").setPredictionCol("prediction").fit(userVector)
    val result = userModel.transform(userVector)
    result.show()
    // result.rdd.saveAsTextFile("D:\\Document\\TestData\\result.txt")
    val clusterCenter = userModel.clusterCenters.map { vector =>
      ( vector(0).toInt,vector(1).toInt)
    }
    val clusterSizes = userModel.summary.clusterSizes
    for ( i <- 0 until clusterCenter.size ){
      println(clusterCenter(i) +"," + clusterSizes(i))
    }
    // 分析用户下车地点中心地带
    // val dropoffRDD = parseData.map { case (id, trip) =>
    //   val longitude = trip.dropoffLoc.getX
    //   val latitude = trip.dropoffLoc.getY
    //   val time = trip.dropoffTime.hour.get().toDouble
    //   (longitude, latitude, time)
    // }
    // dropoffRDD.take(20).foreach(println)
    // val dropoffDataFrame = sqc.createDataFrame(dropoffRDD).toDF("longitude", "latitude", "time")
    // dropoffDataFrame.show()
    // val va = new VectorAssembler().setInputCols(Array("longitude", "latitude", "time")).setOutputCol("features")
    // val dropoffDfVector = va.transform(dropoffDataFrame)

    // 肘部法则确定 k 的最优值
    // val kRange = 10 to 200 by 10
    // val kScore = sc.makeRDD(kRange.map{ k =>
    //   val avgDistance = KmeanUtil.evaluate(k,dropoffDfVector)
    //   k + ":" + avgDistance
    // }.toArray)
    // HDFSDao.save(new Path("hdfs://jane:9000/traffic-dataview/kmeans="+nowTime),kScore)

    // val model = new KMeans().setK(35).setFeaturesCol("features").setPredictionCol("prediction").fit(dropoffDfVector)
    // val result = model.transform(dropoffDfVector)
    // result.show()
    // val clusterCenter = model.clusterCenters.map { vector =>
    //   (vector(0).formatted("%.6f"), vector(1).formatted("%.6f"), vector(2).formatted("%.1f"))
    // }
    // clusterCenter.foreach(println)
    // val clusterSizes = model.summary.clusterSizes
    // clusterSizes.foreach(println)

  }
}
