package day0530

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/30.
  */
object AdvUrlCount {

  def main(args: Array[String]): Unit = {
    val arr = Array("java.itcast.cn","php.itcast.cn","net.itcast.cn")
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
//rdd1 将数据进行切分,元祖中放的是(URL,1)
    //rdd1 将数据进行切分,元祖中放的是(URL,1)
    val rdd1 = sc.textFile("c://itcast.log").map(lint => {
      val f = lint.split("\t")
      (f(1),1)
    })
    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(t=>{
      val url = t._1
      val host = new URL(url).getHost
      (host,url,t._2)
    })
   // val rddjava = rdd3.filter(_._1 == "java.itcast.cn")
   // print(rddjava.collect().toBuffer)
   /* val sordjava = rddjava.sortBy(_._3,false).take(3)
    print(sordjava.toBuffer)*/

    for(ins<- arr){
      val rdd = rdd3.filter(_._1 == ins)
      val resulr = rdd.sortBy(_._3,false).take(3)
      //通过jdbc向数据库存储数据
      //id,学院,url,次数,访问日期
      print(resulr.toBuffer)
    }
    sc.stop()
  }
}
