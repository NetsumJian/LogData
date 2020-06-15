package haofei.utils

import haofei.domain.ActivePlayer
import org.apache.hadoop.fs.shell.find.Result
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.json4s.jackson.Serialization

object HBaseUtil {

  implicit val formats=org.json4s.DefaultFormats

  def queryByRangeAndRegex(sc: SparkContext, startTime: Long, endTime: Long, regex: String) = {
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hbaseConf.set(TableInputFormat.INPUT_TABLE,"sxgf_player")

    val scan = new Scan()
    scan.setStartRow(startTime.toString.getBytes())
    scan.setStopRow(endTime.toString.getBytes())

    val filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(regex))
    scan.setFilter(filter)

    hbaseConf.set(TableInputFormat.SCAN,Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    val resultRDD = sc.newAPIHadoopRDD(hbaseConf,
      classOf[TableInputFormat],classOf[ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    resultRDD
  }

  def saveToHBase(sc: SparkContext,playerList:List[ActivePlayer]): Unit = {
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","hadoop1,hadoop2,hadoop3")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"test_player")

    val job = Job.getInstance(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
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
