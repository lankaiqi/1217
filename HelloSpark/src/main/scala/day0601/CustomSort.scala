package day0601

import org.apache.spark.{SparkConf, SparkContext}

object OrderContext{
  implicit object GirlOrdering extends Ordering[Girl]{
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue>y.faceValue) 1
      else if (x.faceValue==y.faceValue){
        if (x.age>y.age)1 else -1
      }else -1
    }
  }
}
/**
  * Created by Administrator on 2018/6/1.
  */
//name faveValue,age
//先比较faceValue ,相同比较age
object CustomSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))
    import OrderContext._
    val rdd2 = rdd1.sortBy(x=>Girl(x._2,x._3),false)
    println(rdd2.collect().toBuffer)



    sc.stop()
  }
}

//第一种方式
//case class Girl( val faceValue:Int,val age:Int)extends Ordered[Girl] with Serializable{
//  override def compare(that: Girl): Int = {
//      if (this.faceValue==that.faceValue){
//        that.age-this.age
//      }else this.faceValue-that.faceValue
//  }
//}

//第二种
case class Girl(faceValue:Int ,age:Int) extends Serializable
