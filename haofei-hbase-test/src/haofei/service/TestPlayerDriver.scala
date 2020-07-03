package haofei.service

import haofei.utils.HBaseUtil
import org.apache.spark.sql.SparkSession

object TestPlayerDriver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.executor.memory", "16g")
      .appName("LostP layer")
      .getOrCreate()
    val sc = spark.sparkContext

    val startRow = "1590203644_GF0SN10000_00000136305"
    val endRow = "1590486948_GF0SN10000_00000138305"

    /*val resultRdd = HBaseUtil.queryByRangeAndRegex(sc, startRow, endRow)
    println(resultRdd.count)*/
    val result = HBaseUtil.queryByRowkey(endRow)
    result.foreach(println)
  }

}
