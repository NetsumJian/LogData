package haofei.service

import haofei.utils.EmailUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

object TslogDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.executor.memory","16g")
      .appName("tslogApp").getOrCreate
    val sc = spark.sparkContext
    val yesterday = new DateTime().toLocalDate.plusDays(-1).toString

    val rdbArray = Array("sxqd")
    val partArray = Array(10)
    for (str <- rdbArray ; part <- partArray ){
      try {
        hadleTslog(str, part,sc,yesterday)
        EmailUtil.sendSimpleTextEmail(s"$str Tslog已处理","task is success ")
      } catch {
        case e: Exception => {
          EmailUtil.sendSimpleTextEmail(s"$str Tslog未成功处理",s"${e.getMessage}")
        }
      }
    }
  }

  def hadleTslog(rdb:String,part:Int,sc:SparkContext,yesterday:String) = {
    val path = "hdfs://test01:9000/weblog/reporttime=2020-05-13"
    val ds = sc.wholeTextFiles(path,part).map(_._2).flatMap(_.split("\n"))

    val parsedata = ds
      .filter(_.matches("^([a-z]+_)+[a-z]+\\|.*"))
      .map { line =>
        val fileName = line.substring(0, line.indexOf("|"))
        val lineData = line.substring(line.indexOf("|") + 1)
        (fileName, lineData)
      }
      .groupByKey

    parsedata.persist()

    val fileNames = parsedata.map(_._1).collect
    val fileDatas = parsedata.map(_._2.toArray).collect

    for (i <- 0 until fileNames.size) {
      sc.makeRDD(fileDatas(i)).saveAsTextFile(s"hdfs://test01:9000/hive_tslog/$rdb/${fileNames(i)}/report_time=$yesterday")
    }

  }
}
