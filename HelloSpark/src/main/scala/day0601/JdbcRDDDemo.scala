package day0601

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/6/2.
  */
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://192.168.172.134:3306/test", "root", "root")
    }
    val jdbcRDD = new JdbcRDD(  sc,  connection,  "SELECT * FROM DBS where db_id >= ? AND db_id <= ?",  1, 4, 2,  r => {
      val id = r.getInt(1)
      val code = r.getString(2)
      (id, code)
    })

    val jrdd = jdbcRDD.collect()
    println(jrdd.toBuffer)
  }
}
