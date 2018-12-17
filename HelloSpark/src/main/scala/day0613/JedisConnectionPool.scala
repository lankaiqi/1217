package day0613
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool{

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  val pool = new JedisPool(config , "192.168.172.134" ,6379)

  def getConnection():Jedis ={
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()
    val r = conn.keys("*")
    val str = conn.get("新年哈皮_1529026580755")
    println(str)
    println(r)
  }

}

