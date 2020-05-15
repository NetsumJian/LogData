package com.haofei.utils

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD

object HDFSUtil {
  val hadoopConf = new Configuration()
  val fs = FileSystem.get(new URI("hdfs://local01:9000"),hadoopConf)
  def save[T](path:Path,result:RDD[T]) = {
    if (fs.exists(path)){
      fs.delete(path,true)
      result.saveAsTextFile(path.toString)
    }else{
      result.saveAsTextFile(path.toString)
    }
  }
}
