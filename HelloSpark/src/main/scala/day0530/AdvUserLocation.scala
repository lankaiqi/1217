package day0530

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/29.
  */
object AdvUserLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd0 = sc.textFile("c://bs_log").map(x=>{
      val fileds = x.split(",")
      val eventType = fileds(3)
      val time = fileds(1)
      val timeLong = if (eventType=="1") -time.toLong else time.toLong
      ((fileds(0),fileds(2)),timeLong)
    })
   // println(rdd0.collect().toBuffer)
    val rdd1 = rdd0.reduceByKey(_+_).map(x=> {
      val mobile = x._1._1
      val lac = x._1._2
      val time = x._2
      (lac,(mobile,time))
    })
   // println(rdd1.collect().toBuffer)
    val rdd2 = sc.textFile("c://lac_info.txt").map(x=>{
      val f = x.split(",")
      //(基站id,(经度,维度))
      (f(0),(f(1),f(2)))
    })
    val rdd3 = rdd1.join(rdd2).map(t=>{
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile,lac,time,x,y)
    })
    val rdd4 = rdd3.groupBy(_._1)
    println(rdd4.collect().toBuffer)
    val rdd5 = rdd4.mapValues(t=>{
      t.toList.sortBy(_._3).reverse.take(2).toIterable
    })
    rdd5.saveAsTextFile("c://out")
    sc.stop()
  }
}
