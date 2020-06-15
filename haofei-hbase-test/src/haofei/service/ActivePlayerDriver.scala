package haofei.service


import haofei.domain.TestDataSource
import haofei.utils.{HBaseUtil, MysqlUtil}
import org.apache.spark.sql.SparkSession


object ActivePlayerDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory","2g")
      .appName("LostP layer")
      .getOrCreate()
    val sc = spark.sparkContext


    implicit val formats=org.json4s.DefaultFormats
    val players = MysqlUtil.getPlayerArray("2020-06-09", TestDataSource)

    HBaseUtil.saveToHBase(sc,players.toList)

  }
}
