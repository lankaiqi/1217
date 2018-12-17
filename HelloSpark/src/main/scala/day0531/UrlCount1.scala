package day0530

import java.net.URL

import org.apache.spark.{HashPartitioner,Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Administrator on 2018/5/30.
  */
object UrlCount1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //rdd1 将数据进行切分,元祖中放的是(URL,1)
    val rdd1 = sc.textFile("c://itcast.log").map(lint => {
      val f = lint.split("\t")
      (f(1),1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)
    //print(rdd2.collect().toBuffer)
    val rdd3 = rdd2.map(t=>{
      val url = t._1
      val host = new URL(url).getHost
        (host,(url,t._2))
    })
   // rdd3.repartition(3).saveAsTextFile("c://out1")
   val ints = rdd3.map(_._1).distinct().collect()
    //println(rdd4.collect().toBuffer)
    val hostParitioner = new HostPartitiones(ints)

    //val rdd4 = rdd3.partitionBy(hostParitioner).saveAsTextFile("c://out2")

    val rdd4 = rdd3.partitionBy(hostParitioner).mapPartitions(it=>{
      it.toList.sortBy(_._2._2).take(3).iterator
    })
    rdd4.saveAsTextFile("c://out2")
  }
}

class HostPartitiones(ints:Array[String]) extends Partitioner{

  val parMap = new mutable.HashMap[String,Int]()
  var count  = 0
  for (i<-ints){
    parMap += (i->count)
    count += 1
  }
  override def numPartitions: Int =ints.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}
