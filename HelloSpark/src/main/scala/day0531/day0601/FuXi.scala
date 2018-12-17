package day0531.day0601

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/6/1.
  */
object FuXi {
  def main(args: Array[String]): Unit = {
    val arr = Array("java.itcast.cn","php.itcast.cn","net.itcast.cn")
    val conf = new SparkConf().setAppName("FuXi").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    val collect = pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect
    println(collect.toBuffer)
  }
}
