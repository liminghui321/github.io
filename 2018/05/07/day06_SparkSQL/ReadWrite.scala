package cn.itcast.sql

import java.util.{Calendar, Date, Properties}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class ReadWrite {

  val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()

  @Test
  def parquet(): Unit ={

    val sourceDF = spark.read.option("header",true).csv("data/BeijingPM20100101_20151231.csv")
    //写入parquet
    //sourceDF.write.mode(SaveMode.Overwrite).parquet("data/pm_parquet")

    //读取parquet文件
    spark.read.parquet("data/pm_parquet").show
  }

  @Test
  def partionBy(): Unit ={
    val sourceDF = spark.read.option("header",true).csv("data/BeijingPM20100101_20151231.csv")

    /*sourceDF.write.mode(SaveMode.Overwrite)
      .partitionBy("year","month")
      .parquet("data/pm_parquet_01")*/

    //只读取指定月份的数据，比如2010-1 2010-2
    //inc_day=2019-09-12
    //hive分区与分桶区别:
    //  分区:一个分区一个文件夹
    //  分桶:
    //  id,name,province,city,inc_day
    // 按照inc_day进行分区：
    // 按照省份进行分桶:
    // 1,aa,广东,深圳,2019-09-12   ----1
    // 1,aa,广东,广州,2019-09-12   ----2
    // 1,aa,湖南,长沙,2019-09-14   ----3
    // 1,aa,广东,深圳,2019-09-14   ----4
    //以上四条数据:
    //  1、2放入2019-09-12的分区中[1、2省份一样，在同一个桶中]，
    //  3、4放入到2019-09-14的分区中[3、4省份不一样，不在同一个桶中]，
    //分区与分桶的好处:
    //   在查询的时候可以减少数据量
    val source01DF = spark.read.parquet("data/pm_parquet_01/year=2010/month=1")
    val source02DF = spark.read.parquet("data/pm_parquet_01/year=2010/month=2")

    source01DF.createOrReplaceTempView("tmp_01")
    source02DF.createOrReplaceTempView("tmp_02")
    /*spark.sql(
      """
        |select *,2010 year,1 month
        | from tmp_01
        |union
        | select *,2010 year, 2 month
        | from tmp_02
      """.stripMargin).show*/
    //
    //source01DF.write.mode(SaveMode.Overwrite).parquet("data/pm_parquet_02/20190912")

    //ALTER TABLE test1 ADD PARTITION(inc_day=20190912) LOCATION 'hdfs://node01:8020/data/pm_parquet_02/20190912'

    //读取一周的数据
    //data/pm_parquet_02/20190911
    //data/pm_parquet_02/20190912
    /**
      * 思路:
      *   根据当前日期-1 = 前一天的日期
      *   根据当前日期-2 = 大前天的日期
      *   .....
      */
    //用于存放一周数据的路径
    var dateList = new ArrayBuffer[String]()

    for(i<- 1 to 7){
      val date = new Date()

      val calenar = Calendar.getInstance()
      calenar.setTime(date)
      //在当前日期上 - i 天
      calenar.add(Calendar.DAY_OF_YEAR,-i)

      val formater = FastDateFormat.getInstance("yyyyMMdd")
      //格式化日期成yyyyMMdd格式字符串
      val dateStr = formater.format(calenar)
      //将日期添加到路径中
      dateList.+=(s"data/pm_parquet_02/${dateStr}")
    }
    //:_*  将集合中的所有元素取出来当做参数放入到方法中
    //读取一周的数据
    spark.read.parquet(dateList:_*)
  }

  @Test
  def readJdbc(): Unit ={
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","root")
    spark.read.jdbc("jdbc:mysql://hadoop01:3306/mysql","user",prop)

      .write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://hadoop01:3306/mysql","user",prop)




  }
}
