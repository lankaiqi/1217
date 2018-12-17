package day0613

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/6/14.
  */
object ScannPlugins {
  def main(args: Array[String]): Unit = {
    val Array(zkQuorm,group,topic,numThreads) = Array("lkq:2181,lkq2:2181,lkq3:2181","g17","zbzb","1")
    val dataFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val conf = new SparkConf().setAppName("ScanPlugins").setMaster("local[2]")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Milliseconds(10000))
    sc.setCheckpointDir("d://ck0")

    val topicMap = topic.split(",").map((_,numThreads.toInt)).toMap
    val kafkaParms = Map[String,String](
      "zookeeper.connect" -> zkQuorm,
      "group.id" -> group,
      "auto.offset.reset" -> "smallest"
    )
    val dstream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParms,topicMap,StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.map(_._2)
    val splitedLines = lines.map(_.split("\t"))
    val filteredLines = splitedLines.filter(f=>{
      val et = f(3)
      val item = dataFormat.parse(f(12)).getTime
     // print(et+"==="+item)
    //  et == "15" && item == "恶魔铃铛"
      true
    })
    val aaaagrouedWindow= filteredLines.map(f => (f(7), dataFormat.parse(f(12)).getTime))
   // println("aaaagrouedWindow============"+aaaagrouedWindow)
    val grouedWindow = aaaagrouedWindow.groupByKeyAndWindow(Milliseconds(300000), Milliseconds(200000))
    //println("============"+grouedWindow)
    val filtered: DStream[(String, Iterable[Long])] = grouedWindow.filter(_._2.size >= 2)
   // println("+++++++++++++++"+filtered)
    val itemAvgTime = filtered.mapValues(it=> {
      val list = it.toList.sorted
      val size = list.size
      println("1111===="+size)
      val first :Long =list(0)
      val last :Long = list(list.size-1)
      val cha: Double = last - first
      cha/ size
    })
    val badUser: DStream[(String, Double)] = itemAvgTime.filter(_._2<1000000000)
  //  println("-------------------"+filtered)
    badUser.foreachRDD(rdd=>{
      rdd.foreachPartition(it => {
        val it1: Iterator[(String, Double)] = it
        val connection = JedisConnectionPool.getConnection()
        it1.foreach(t=>{
        //  println(t._1.toString)
          val user = t._1
      //    println("222===="+user)
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user+"_"+currentTime,avgTime.toString)
        })
        val  aa = connection.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
