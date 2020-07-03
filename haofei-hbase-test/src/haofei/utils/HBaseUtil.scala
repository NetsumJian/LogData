package haofei.utils

import java.time.LocalDate

import haofei.domain.ActivePlayer
import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos.ByteArrayComparable
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer

object HBaseUtil {

  implicit val formats=org.json4s.DefaultFormats

  def queryByRowkey(rowKey:String, startDay:LocalDate, endDay:LocalDate): ArrayBuffer[ActivePlayer] ={
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")

    import org.apache.hadoop.hbase.client.HTable
    val table = new HTable(hbaseConf, "sxgf_player")
    // 获取指定行键的数据
    val get = new Get(rowKey.getBytes)
    // 获取结果
    val r = table.get(get)
    var d = startDay
    val playerArray = new ArrayBuffer[ActivePlayer]()
    while (d.toString < endDay.toString){
      val bs = r.getValue("active_info".getBytes,d.toString.getBytes)
      if ( null != bs){
        val player = Serialization.read[ActivePlayer](new String(bs))
        playerArray.append(player)
      }
      d = d.plusDays(1)
    }

    playerArray

  }

  def queryByRangeAndRegex(sc: SparkContext, startRow: String, endRow: String) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"sxgf_player")

    val scan = new Scan()
    scan.setStartRow("0".getBytes())
    scan.setStopRow(endRow.getBytes())
    new RowFilter(CompareOp.EQUAL,new RegexStringComparator("^1590486948.*"))

    // val filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex))
    // scan.setFilter(filter)

    hbaseConf.set(TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    val resultRDD = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    resultRDD
  }

  def saveToHBase(sc: SparkContext,playerList:List[ActivePlayer]): Unit = {
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"test_player")

    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[org.apache.hadoop.hbase.client.Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val r1 = sc.makeRDD(playerList)
    val hbaseRDD = r1.map{ player =>
      val rowKey = player.registerTime + "_" +player.channelId + "_" + player.accountId
      val row = new Put(rowKey.getBytes())
      val daily = Serialization.write(player)
      row.add("active_info".getBytes(),player.date.getBytes(),daily.getBytes())
      (new ImmutableBytesWritable(),row)
    }

    hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)

  }

}
