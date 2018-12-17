package day0603

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Administrator on 2018/6/3.
  */
object SqlDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLDemo")//.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    System.setProperty("user.name","hadoop")

    val personRDD= sc.textFile("hdfs://lkq:8020/person.txt").map(line=>{
        val fields = line.split(",")
      Person(fields(0).toLong,fields(1),fields(2).toInt)
    })
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    personDF.registerTempTable("person")
    sqlContext.sql("select * from person where age>=28 order by age desc limit 2").show()

    sc.stop()
  }
}

case class Person(id:Long,name:String,age:Int)
