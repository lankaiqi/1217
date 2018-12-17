package day0607

import day0606.LoggerLevels
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/6/7.
  */
object WorkCountStream {
  /**
    * String : 单词 hello
    * Seq[Int] ：单词在当前批次出现的次数
    * Option[Int] ： 历史结果
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }

  def main(args: Array[String]): Unit = {

    //LoggerLevels.setStreamingLogLevels()
    //StreamContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //接收数据
    //updateStateByKey必须设置checkooint
    val ds = ssc.socketTextStream("lkq", 8888)
    sc.setCheckpointDir("d://aaa")
    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).updateStateByKey(updateFunc,new HashPartitioner(sc.defaultParallelism),true)
    //打印结果
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
