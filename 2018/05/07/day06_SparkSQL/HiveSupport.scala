package cn.itcast.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object HiveSupport {

  def main(args: Array[String]): Unit = {

    //1、创建SparkSession，指定warehouse与metastroe
    val spark = SparkSession.builder()
      .appName("test")
      .config("spark.sql.warehouse.dir","")
      .config("hive.metastore.uris","")
    //2、开启hive支持
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use myhive")

    val df = spark.createDataFrame(Seq(
      ("09","zhangsan1","1990-01-20","man"),
      ("10","zhangsan2","1990-01-21","man"),
      ("11","zhangsan3","1990-01-22","man"),
      ("12","zhangsan4","1990-01-23","man")
    ))
    //在写入hive的时候 dataFrame的列名必须要和表的列名一致，否则会报错
    df.toDF("id","name","local_date","sex")
      .write
      //如果hive表不是在spark代码中创建的而是在hive shell命令行中创建的，那么在插入的数据的时候可能会报错
      //此时需要通过format(Hive)解决
      .format("Hive")
      .saveAsTable("student")
  }
}
