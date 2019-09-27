package com.itheima.spark.farmproducts

import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

import scala.util.Try

/**
  * 1、农产品市场个数统计
  * 1． 统计每个省份的农产品市场总数
  * 2． 统计没有农产品市场的省份有哪些
  * 2、农产品种类统计
  * 1． 根据农产品类型数量，统计排名前 3 名的省份
  * 2． 根据农产品类型数量，统计每个省份排名前 3 名的农产品市场
  * 3、价格区间统计,
  *   1. 计算山西省的每种农产品的价格波动趋势，即计算每天价格均值
  */
class FarmProducts extends Serializable {

  // 封装Bean
  case class Product(name: String, price: Float, crawlTime: Date, market: String, province: String, city: String)

  //创建sparkSession
  val spark: SparkSession = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
  //农产品的数据集
  val productRdd1: RDD[String] = spark.sparkContext.textFile("dataset/Farmproducts/product.txt")

  //省份的数据集
  val provinceRdd1 = spark.read.textFile("dataset/Farmproducts/allprovince.txt").toDF("province")

  //导入隐式转换
  import spark.implicits._
  /**
    * * 1、农产品市场个数统计
    * 1． 统计每个省份的农产品市场总数
    * 2． 统计没有农产品市场的省份有哪些
    */
  @Test
  def marketAmount(): Unit = {

    //农产品数据
    val productDF1: DataFrame = productRdd1.map(line => {
      line.split("\t")
    })
      //过滤出不足6个字段的数据
      .filter(_.length == 6)
      //转换为6列元祖
      .map(arr => (arr(0), arr(1), arr(2), arr(3), arr(4), arr(5)))
      //name,price,crawl_time,market,province,city
      .toDF("name", "price", "crawl_time", "market", "province", "city")
    productDF1.createOrReplaceTempView("product_tab")

    //1.1统计每个省份的农产品市场总数
    spark.sql(
      """
        |select province,count(distinct market) marketAmount
        | from product_tab
        |group by province
        |having marketAmount>5
      """.stripMargin)//.show

    //身份数据集
    provinceRdd1.createOrReplaceTempView("province_tab")

    //1.2统计没有农产品市场的省份有哪些
    //select from a
    spark.sql(
      """
        |select a.* from
        |province_tab a
        |left join
        |product_tab b
        |on a.province = b.province
        |where b.province is null
      """.stripMargin)//.show()
  }

  /**
    * 2、农产品种类统计
    *   1． 根据农产品类型数量，统计排名前 3 名的省份
    *   2． 根据农产品类型数量，统计每个省份排名前 3 名的农产品市场
    */

  @Test
  def farmCount(): Unit ={
    //农产品数据
    val productDF1: DataFrame = productRdd1.map(line => {
      line.split("\t")
    })
      //过滤出不足6个字段的数据
      .filter(_.length == 6)
      //转换为6列元祖
      .map(arr => (arr(0),arr(1) , arr(2), arr(3), arr(4), arr(5)))
      //name,price,crawl_time,market,province,city
      .toDF("name", "price", "crawl_time", "market", "province", "city")
    productDF1.createOrReplaceTempView("product_tab")


//    查询
    //2.1农产品类型数量，统计排名前 3 名的省份
    spark.sql(
      """
        |select province,count(1) as farmCount
        |from product_tab
        |group by province
        |order by farmCount desc
        |limit 3
      """.stripMargin)//.show

    //2.2根据农产品类型数量，统计每个省份排名前 3 名的农产品市场
    //
    spark.sql(
      """
        |select *
        |from
        |(select province,market,rank() over(PARTITION BY province ORDER BY count(*) DESC) as rank
        |from product_tab
        |group by province,market) tmp
        |where rank <= 3
      """.stripMargin).show
  }

  @Test
  def avg_price(): Unit ={
    //3. 1. 计算山西省的每种农产品的价格波动趋势，即计算每天价格均值
    //山西   农产品  每天   价格均价
    //农产品数据
    val productDF1: DataFrame = productRdd1.map(line => {
      line.split("\t")
    })
      //过滤出不足6个字段的数据
      .filter(_.length == 6)
      //转换为6列元祖
      .map(arr => (arr(0), Try(arr(1).toDouble).getOrElse(0D), arr(2), arr(3), arr(4), arr(5)))
      //name,price,crawl_time,market,province,city
      .toDF("name", "price", "crawl_time", "market", "province", "city")
    productDF1.createOrReplaceTempView("product_tab")

    spark.sql(
      """
        |select crawl_time,name,(sum(price)-max(price)-min(price))/(count(price)-2) as avg_price
        |from product_tab
        |where province="山西"
        |group by name,crawl_time
        |order by crawl_time
      """.stripMargin).show()

  }

  @Test
  def test1(): Unit = {
    //添加一列ID
    import org.apache.spark.sql.functions.monotonically_increasing_id
    val provinceDF: DataFrame = provinceRdd1.withColumn("id", monotonically_increasing_id)
    provinceDF.show()
  }
}
