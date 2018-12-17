package day0530

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/29.
  */
object UserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
   // val stringses = sc.textFile("c://bs_log").map(_.split(",")).map(x=>(x(0),x(1),x(2),x(3)))
  //  println(stringses.collect().toBuffer)
    val mbt = sc.textFile("c://bs_log").map(x=>{
    val fileds = x.split(",")
    val eventType = fileds(3)
    val time = fileds(1)
    val timeLong = if (eventType=="1") -time.toLong else time.toLong
    (fileds(0)+"_"+fileds(2),timeLong)
  })
    //println(mbt.collect().toBuffer)
    //(18611132889_9F36407EAD0629FC166F14DDE7970F68,54000)
    val rdd1 = mbt.groupBy(_._1).mapValues(_.foldLeft(0L)(_+_._2))
   // println(rdd1.collect().toBuffer)
    val rdd2 = rdd1.map(x=>{
      val mobile_bs = x._1.split("_")
      val mobile = mobile_bs(0)
      val lac = mobile_bs(1)
      val time = x._2
      (mobile,lac,time)
    })
    val rdd3 = rdd2.groupBy(_._1)
    //println(rdd3.collect().toBuffer)
    val rdd4 = rdd3.mapValues(it => {
      it.toList.sortBy(_._3).reverse.take(2)

    })
    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}
