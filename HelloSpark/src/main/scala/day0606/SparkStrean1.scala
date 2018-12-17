package day0606

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2018/6/6.
  */
object SparkStrean1 {
  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()
    //StreamContext
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    //接收数据
    val ds = ssc.socketTextStream("lkq", 8888)
    //DStream是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //打印结果
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
