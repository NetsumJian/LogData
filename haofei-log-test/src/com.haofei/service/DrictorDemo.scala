package com.haofei.service

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object DrictorDemo{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("kafkaWC").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //创建SparkStreaming，并设置时间间隔
    val ssc = new StreamingContext(sc,Seconds(5))

    val group = "groupTT"//指定组名
    //指定消费的topic名字
    val topic = "sxgf_tslog"
    //指定kafka的broker地址，Streaming的Task直连到kafka的分区上，用底层的API，效率更高
    val brokerList = "hadoop1:9092,hadoop2:9092,hadoop3:9092"
    //指定zk地址，后期更新消费的偏移量时使用（以后可以Redis、MySQL）
    val zkQuorum = "hadoop1:2181,hadoop2:2181,hadoop3:2181"
    //创建DStream时使用topic名字的集合，SparkStreaming可以同时消费多少topic
    val topics:Set[String] = Set(topic)
    //创建一个ZKGroupTopicDirs对象，其实就是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(group,topic)
    //获取zookeeper中的路径"groupTT/offsets/tt01"

    val zkTopicPath:String = s"${topicDirs.consumerOffsetDir}"

    //准备kafka参数
    val kafkaParams = Map(
      "metadata.broker.list"->brokerList,
      "group.id"->group,
      "auto.offset.reset"->kafka.api.OffsetRequest.SmallestTimeString
    )
    //zookeeper的host和ip，创建一个Client，用于更新偏移量
    //是zookeeper的一个客户端，可以从zk中读取，偏移量的数据，并跟新偏移量
    val zkClient: ZkClient = new ZkClient(zkQuorum)
    //查询该路径下是否字节点（默认有字节点为我们自己保存不同Partition生成的）
    //   /consumers/组名/offsets/topic名/分区名/偏移量, 可以zkClient.sh 插询
    val children = zkClient.countChildren(zkTopicPath)

    //创建一个InputDStream, 要是var，因为不去定是不是以前读过，要先判断，再赋值
    //key 是 kafka的Key，默认不设置是null，value是读取的内容
    var kafkaStream:InputDStream [(String,String)] = null

    //如果zookeeper中保存offset，我们会利用这个Offset作为kafkaStream的读取位置
    var fromOffsets:Map[TopicAndPartition,Long] = Map()

    //如果保存过Offset，以前读取过
    if(children >0){
      for (i<- 0 until children){
        //zkClient根据文件位置读取偏移量( /consumers/组名/offsets/topic名/分区名/偏移量,)
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/${i}")
        val tp: TopicAndPartition = TopicAndPartition(topic,i)
        //将不同 Partition对应的Offset增加到fromOffset中（
        //
        fromOffsets += (tp-> partitionOffset.toLong)
        //这个会将kafka的消息进行transform，最终的kafka的数据会变成kafka的key，message）这样的Tuple
        val messageHandler = (mam:MessageAndMetadata[String,String])=>(mam.key(),mam.message())
        //通过KafkaUtils创建直连的DStream，fromOffset参数的作用是按照之间计算好的偏移量继续读取
        //[String,String,StringDecoder,StringDecoder,(String,String)]
        // key    value   key的解码，   value的解码
        kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](
          ssc,kafkaParams,fromOffsets,messageHandler
        )
      }
    }else{
      //从头开始读，之前没有读取过
      kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    }
    var offsetRanges = Array[OffsetRange]()

    //从kafka中读取消息，DStream的Transform方法，可以将当前批次RDD取出来来
    //该transform方法计算获取当前批次RDD，然后将RDD的偏移量取出来，然后将RDD返回DStream
    val transformed: DStream[(String, String)] = kafkaStream.transform(rdd => {
      //得到该rdd对应的kafka的消息的offset
      //该RDD是个kafkaRDD，可以获取偏移量的范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val messages: DStream[String] = transformed.map((_._2))

    //一次迭代DStream中的RDD
    messages.foreachRDD(rdd=>{
      //对RDD进行操作，触发Action
      rdd.foreachPartition(partition=>{
        partition.foreach(x=>{
          println(x)
        })
      })
    })

    for(off <- offsetRanges){
      //获取zk 中记录偏移量的目录，
      //    /consumers/组名/offsets/topic名/分区名
      val zkPath = s"${topicDirs.consumerOffsetDir}/${off.partition}"
      //更新偏移量
      // ZkUtils.updatePersistentPath(zkClient,zkPath,off.untilOffset.toString)
    }

    ssc.start()
    ssc.awaitTermination()

  }

}