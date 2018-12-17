package scalaErCiPaiXu

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/7/22.
  */
object SortKeyTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortKeyTest").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val arr = Array(Tuple2(new SortKey(30,35,50),"1"),
      Tuple2(new SortKey(35,30,40),"2"),
      Tuple2(new SortKey(30,35,40),"3"))

    val rdd= sc.parallelize(arr,1)

    val sortedRdd = rdd.sortByKey(false)

    for (tuple <- sortedRdd){
      println(tuple._2)
    }
  }
}
