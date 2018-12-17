package day0601

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/6/1.
  */
object IpLocation {

  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps : PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
      conn = DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test", "root", "root")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocation")
    val sc = new SparkContext(conf)

    val ipRulesRdd   = sc.textFile("d://ip.txt").map(ling => {
      val fields = ling.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num,end_num,province)
    })
    //全部的ip映射规则
    val ipRulesArray = ipRulesRdd.collect()
    //广播规则
    val ipRules = sc.broadcast(ipRulesArray)
    //加载要处理的数据
    val ipsRDD = sc.textFile("d://access_log").map(line => {
      val fields = line.split("\\|")
      fields(1)
    })
    val result = ipsRDD.map(ip=>{
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRules.value,ipNum)
      val info = ipRules.value(index)
      info
    })
   /* val rdd = result.groupBy(_._3).map(x=>{
      val list = x._2.toList
      var a=0
      val str = list.map(x=>{
        a=a+1
      })
      ( x._1, a)
    })*/
   val rdd = result.map(t=>(t._3,1)).reduceByKey(_+_)
    //向MySQL写入数据
    rdd.foreachPartition(data2MySQL(_))

    println (rdd.collect().toBuffer)
  }
}






















