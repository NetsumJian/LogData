package com.haofei.testsheet

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/**
 * 测试sparkContext 案例
 */
object TestOfSparkContextOnYarn {


  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "E:\\soft\\winutils\\hadoop-common-2.2.0-bin-master")
    /*val spark = SparkSession.builder()
      .appName("TestOfSparkContext2OnYarn")
      .master("local[2]").getOrCreate()*/
    val path = "/user/hadoop/data/test-data-forSpark/emp-forSpark.txt"
    val spark = SparkSession.builder().getOrCreate()


    //反射的方式 RDD=>DF
    // reflection(spark,path)
    //编程的方式 RDD=>DF
    program(spark,path)
    spark.stop()
  }

  private def reflection(spark: SparkSession,path:String ) = {
    val rdd = spark.sparkContext
      .textFile(path)
      .map(w => w.split("\t"))

    import spark.implicits._
    val empDF = rdd.map(line =>
      EMP(line(0), line(1), line(2), line(3),
        line(4), line(5), line(6), line(7))).toDF()
    empDF.printSchema()
    empDF.show()
  }

  def program(spark: SparkSession,path:String) = {
    val infoRdd = spark.sparkContext
      .textFile(path)
      .map(w => w.split("\t")).map(line => Row(line(0), line(1), line(2), line(3), line(4), line(5), line(6), line(7)))


    val filedName = Array(StructField("empNo", StringType, true)
      , StructField("ename", StringType, true), StructField("job", StringType, true)
      , StructField("mgr", StringType, true), StructField("hireDate", StringType, true)
      , StructField("sal", StringType, true), StructField("comm", StringType, true)
      , StructField("deptNo", StringType, true))

    val schema = StructType(filedName)
    val empDf = spark.createDataFrame(infoRdd, schema)
    empDf.printSchema()
    empDf.show(false)

    //注册临时表
    empDf.createOrReplaceTempView("emp")
    val sqlDF = spark.sql("select ename,job,comm from emp")
    // sqlDF.printSchema()
    //sqlDF.show()
    /*empDf.write.format("json")
      .mode(SaveMode.Overwrite).save("E:///testData/empTable3")*/
    empDf.coalesce(1).write.format("json").mode(SaveMode.Overwrite)
      .partitionBy("deptno").save("hdfs://hadoop001:9000/user/hadoop/emp-spark-test/emp-jsonOnYarnTable")
  }

  case class EMP(empNo: String, empName: String, job: String, mgr: String, hiredate: String,
                 sal: String, comm: String, deptNo: String)

}

