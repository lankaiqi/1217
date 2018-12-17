package day0528

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/28.
  */
object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ForeachDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
	aaaaaaaaa
   // rdd1.foreach(println(_))
    rdd1.foreachPartition(it=>{
      print(it)
    })
    sc.stop()

  }
}
