import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/5/27.
  */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WC").setJars(Array("G:\\dashuju\\HelloSpark\\target\\hello-spark-1.0-SNAPSHOT.jar")).setMaster("spark://lkq:7077")
    val sc = new SparkContext(conf)
   // sc.textFile("hdfs://lkq:50070/work").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))

    //textFile会产生两个RDD：HadoopRDD  -> MapPartitinsRDD
   val aaa =  sc.textFile("hdfs://lkq:8020/work")
      // 产生一个RDD ：MapPartitinsRDD
      .flatMap(_.split("\t"))
      //产生一个RDD MapPartitionsRDD
      .map((_, 1))
      //产生一个RDD ShuffledRDD

      //产生一个RDD: mapPartitions
      .saveAsTextFile("hdfs://lkq:8020/work24")
    //println(aaa.collect().toBuffer)
//println(aaa.collect().toBuffer)
   //println(aaa.collect().toBuffer)
    sc.stop()
  }
}
