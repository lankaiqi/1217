package day0530

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/30.
  */
object UrlCount {
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
      (host,url,t._2)
    })
    //print(rdd3.collect().toBuffer)
    val rdd4 = rdd3.groupBy(_._1)
    //print(rdd4.collect().toBuffer)
    val rdd5 = rdd4.mapValues(t=>{
      t.toList.sortBy(_._3).reverse.take(3).toIterable
    })
    print(rdd5.collect().toBuffer)
    rdd5.saveAsTextFile("c://out2")
    sc.stop()
  }
}
