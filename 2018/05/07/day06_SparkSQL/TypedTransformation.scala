package cn.itcast.sql

import org.apache.spark.sql.{Dataset, SparkSession}
import org.junit.Test

/**
  * distinct
  * filter
  * selectExpr
  */
class TypedTransformation {

  val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()
  import spark.implicits._
  @Test
  def as(): Unit ={
    //1、读取数据
    val sourceDF = spark.read.option("sep","\t").csv("data/studenttab10k")
      .toDF("name","age","gpa")

    //在转化的时候，df的列名要与case class的属性名一致
    val ds: Dataset[Student] = sourceDF.as[Student]

    ds.show
  }

  @Test
  def distinct(): Unit ={
    val ds1 = spark.createDataset(Seq(("aa",20),("aa",30),("bb",20),("bb",30)))
      .toDF("name","age")
    //对指定列的值重复的行进行去重，如果传入多个列名，那么这多个列的值都要相同才会去重
    //ds1.dropDuplicates("name").show

    ds1.createOrReplaceTempView("tmp_table")

    /**
      * aa 20
      * bb 20
      */
    spark.sql(
      """
        |select t.name,t.age from(
        |select name,age,row_number() over(partition by name order by age desc) rn
        | from tmp_table) t
        | where t.rn=1
      """.stripMargin).show

    /**
      * aa 20
      * aa 30
      *
      * bb 20
      * bb 30
      *
      */

    /**
      * aa 30  1
      * aa 20  2
      *
      * bb 30  1
      * bb 20  2
      */
  }

  @Test
  def test(): Unit ={

    val class_01 = spark.createDataset(Seq(("aa","20"),("bb","30"),("cc","20"),("dd","30"))).toDF("name","age")
    val class_02 = spark.createDataset(Seq(("aa","20"),("bb","30"),("ee","20"),("ff","30"))).toDF("name","age")

    class_01.selectExpr("name","cast(age as bigint) as age").printSchema()

    class_01.createOrReplaceTempView("class_01")
    class_02.createOrReplaceTempView("class_02")
    //用sql语句得到存在与class_01中而不在class_02中的人
    spark.sql(
      """
        |select a.*
        | from class_01 a left join class_02 b
        | on a.name = b.name
        | where b.name is null
      """.stripMargin)//.show

    /**
      * aa 20
      * bb 30
      * cc 20
      * dd 30
      *
      * left join
      *
      * aa 20
      * bb 30
      * ee 20
      * dd 30
      *
      * aa 20 aa 20
      * bb 30 bb 30
      * cc 20 null null
      * dd 30 null null
      */

    //交集 即存在class_01有存在class_02中
    spark.sql(
      """
        |select a.*,b.*
        | from class_01 a join class_02 b
        | on a.name=b.name
      """.stripMargin)//.show

    //并集
    spark.sql(
      """
        |select  *
        | from class_01
        | union
        | select * from class_02
      """.stripMargin)//.show
  }
}

case class Student(name:String,age:String,gpa:String)