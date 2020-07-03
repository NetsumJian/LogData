package com.haofei.service

import java.text.SimpleDateFormat

import breeze.linalg.DenseMatrix
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import nak.cluster.{DBSCAN, GDBSCAN, Kmeans}

object KmeanDriver {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("user-kmeans")
    val sc = new SparkContext(conf)
    val sqc = new SQLContext(sc)
    val nowTime = new DateTime().toLocalDate
    val dataPath = "D:\\Document\\WorkDoc\\user\\sxgf_user_info.csv"
    val userData = sc.textFile(dataPath)
    // userData.take(20).foreach(println)
    val parseData = userData.zipWithIndex.filter(_._2 > 1)
      .map { case (line, index) =>
        val user = line.split(",")
        (
          user(0).toDouble,
          user(1).toDouble,
          user(2).toDouble,
          user(3).toDouble,
          user(4).toDouble,
          user(5).toDouble,
          user(6).toDouble,
          user(7).toDouble,
          user(12).toDouble - user(8).toDouble,
          user(9).toDouble,
          user(10).toDouble,
          user(11).toDouble,
          user(13).toDouble,
          user(14).toDouble,
          user(15).toDouble
        )
      }.collect()

    val userMatrix = DenseMatrix(parseData: _*)
    val model = new GDBSCAN(DBSCAN.getNeighbours(epsilon = 0.0005f, Kmeans.euclideanDistance),
      DBSCAN.isCorePoint(30))
    val clusters = model.cluster(userMatrix)

    clusters.foreach(println)
    /* K-Means 算法
    // parseData.take(20).foreac h(println)
    val userDF = sqc.createDataFrame(parseData).toDF("uid", "gold", "payGold", "diamond",
      "lottery", "level", "exp", "vip", "time", "factor", "lastSign", "charged", "energy", "vipExp", "energyFactor")

    val userVector = new VectorAssembler()
      .setInputCols(Array("gold", "payGold", "diamond",
        "lottery", "level", "exp", "vip", "time", "factor",
        "lastSign", "charged", "energy", "vipExp", "energyFactor"))
      .setOutputCol("features")
      .transform(userDF)

    val model = new KMeans()
      .setK(35)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      .fit(userVector)

    val clusterSizes = model.summary.clusterSizes
    clusterSizes.foreach(println)
*/
  }
}
