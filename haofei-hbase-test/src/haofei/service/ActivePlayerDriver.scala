package haofei.service


import haofei.domain.TestDataSource
import haofei.utils.{HBaseUtil, MysqlUtil}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import com.github.nscala_time.time.Imports._

object ActivePlayerDriver {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory", "2g")
      .appName("LostP layer")
      .getOrCreate()
    val sc = spark.sparkContext
    val today = new DateTime().toLocalDate
    var startDay = new DateTime("2020-03-21").toLocalDate

    implicit val formats = org.json4s.DefaultFormats
    while(startDay.toString < (today-1.day).toString ){
      val players = MysqlUtil.getPlayerArray(startDay.toString, TestDataSource)
      HBaseUtil.saveToHBase(sc, players.toList)
      startDay+=1.day
    }
  }
}
