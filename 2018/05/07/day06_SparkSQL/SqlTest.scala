package cn.itcast.sql

import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql._
import org.junit.Test

case class Person(id:Int,name:String,age:Int)

class SqlTest {
  /**
    * 命令式
    */
  @Test
  def test1(): Unit ={

    //1、创建SparkSession对象
    val spark = SparkSession.builder().master("local[6]").appName("test").getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //2、创建数据
    val data: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Seq((1,"张三",20),(2,"李四",30),(3,"王五",25)))

    //3、将rdd转成dataset/dataframe
    //通过反射的方式进行转换，将属性名当成列名
    //val ds: Dataset[(Int, String, Int)] = data.toDS()
    //val ds =data.toDF("id","name","age").where('age >=25)
    val ds =data.toDF("id","name","age").where("age >=25")

    ds.show()
  }

  /**
    * 声明式
    */
  @Test
  def test2(): Unit ={
    //1、创建SparkSession对象
    val spark = SparkSession.builder().master("local[6]").appName("test").getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //2、创建数据
    val data: RDD[(Int, String, Int)] = spark.sparkContext.parallelize(Seq((1,"张三",20),(2,"李四",30),(3,"王五",25)))

    data.toDF("id","name","age").createOrReplaceTempView("tmp_table")

    //select * from table where age>=25
    spark.sql(
      """
        |select *
        | from tmp_table where age>=25
      """.stripMargin).show
  }

  @Test
  def test3(): Unit ={
    //1、创建SparkSession对象
    val spark = SparkSession.builder().master("local[6]").appName("test").getOrCreate()

    //导入隐式转换
    import spark.implicits._
    //2、创建数据
    val data: RDD[Person] = spark.sparkContext.parallelize(Seq(Person(1,"张三",20),Person(2,"李四",30),Person(3,"王五",25)))

    val ds = data.toDS()
    val frame = data.toDF()
    //强类型[在编译器就会检测]
    ds.filter(item=> item.age >10).show
    //弱类型[在运行期才会检测]
    ds.filter("address>10").show

    //2.0之前没有dataset
    //1、运行才能检测，错误不能及时发现
    //2、dataFrame不能够处理非结构化的数据
    //  1、直接用dataset处理成结构化数据
    //  2、转换为rdd之后处理成结构化数据
    //val rdd1: RDD[Person] = ds.rdd
  }

  /**
    * 创建DataSet三种方式
    */
  @Test
  def createDataSet(): Unit ={
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()

    import spark.implicits._
    //1、toDS
    val data: RDD[Person] = spark.sparkContext.parallelize(Seq(Person(1,"张三",20),Person(2,"李四",30),Person(3,"王五",25)))
    data.toDS()
    //2、createDataSet
    val ds1 = spark.createDataset(data)
    //3、读取文本文件
    val ds2 = spark.read.textFile("data/access_log_sample.txt")
    val ds3: Dataset[(String, String)] = ds2.map(item=>{
      val arr = item.split(" ")
      val ip = arr(0)
      val time = arr(3).substring(1)
      (ip,time)
    })
    ds3.toDF("ip","local_time").show
  }

  @Test
  def test4(): Unit ={
    //1、创建SparkSession
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()

    //2、读取文件
    /** csv常用option
      * sep：分隔符,默认是,
      * header：读取的csv文件中是否有头信息
      * inferSchema：是否推断字段类型
      */
    val sourceDF = spark.read.option("header","true")
      .option("inferSchema",true)
      .csv("data/BeijingPM20100101_20151231.csv")


    import spark.implicits._
    val df1= sourceDF.select('year,'month,'PM_Dongsi)
      .where("PM_Dongsi!='NA'")
      .groupBy('year,'month)
      .count()

    df1.show()
  }

  @Test
  def row(): Unit ={
    val spark = SparkSession.builder().master("local[3]").appName("test").getOrCreate()

    import spark.implicits._
    //1、toDS
    val data: RDD[Person] = spark.sparkContext.parallelize(Seq(Person(1,"张三",20),Person(2,"李四",30),Person(3,"王五",25)))

    val df1 = data.toDF()
    //需求:将每一个age*10

    df1.rdd.map(row=>{
      //row.getAs[列值的类型](列名)
      val name = row.getAs[String]("name")
      val id = row.getAs[Int]("id")
      val age = row.getAs[Int]("age")
      (id,name,age*10)
    }).toDF("id","name","age").show

    //parquet文件自带schema，是列式存储
    df1.write.mode(SaveMode.Append).parquet("")
  }

}
