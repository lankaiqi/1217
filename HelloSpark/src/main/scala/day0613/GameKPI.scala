package day0613

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/6/13.
  */
object GameKPI {

  def main(args: Array[String]): Unit = {

    val queryTime = "2016-02-02 00:00:00"
    //val endTime =   "2016-02-02 00:00:00"
    val beginTime = TimeUtils(queryTime)
    val endTime = TimeUtils.getCertainDayTime(+1)

    val conf = new SparkConf().setAppName("GameKPI").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //切分之后的数据
    val splitedLogs = sc.textFile("d://GameLog.txt").map(_.split("\\|"))
    //过滤后并缓存
    val filteredLogs = splitedLogs.filter(fields => FilterUtils.filtetByTime(fields,beginTime,endTime)).cache()

    //日新增用户 DNU
    val dnu = filteredLogs.filter(fields => FilterUtils.filterByType(fields,EventType.REGISTER)).count()
    println(dnu)

    //日活跃用户 DAU
    val dau = filteredLogs.filter(fields => FilterUtils.filterByTypes(fields,EventType.REGISTER,EventType.LOGIN)).map(_ (3)).distinct().count()
    println(dau)

    //留存率 某段时间新增的用户数记为A,经过一段时间仍然使用的用户占新增用户A的比例即为留存率
    //次日留存率 日新增用户在+1日登陆的用户占新增用户的比例
    val t1 = TimeUtils.getCertainDayTime(-1)
    val lastDayRegUser = splitedLogs.filter(fields => FilterUtils.filterByTypeAndTime(fields,EventType.REGISTER,t1,beginTime)).map(x=> (x(3),1))

    val todayLogininUser = filteredLogs.filter(fields => FilterUtils.filterByType(fields,EventType.LOGIN)).map(x=> (x(3),1)).distinct()
    val dr:Double = todayLogininUser.join(lastDayRegUser).count()
    println(dr)
    val drr = dr / lastDayRegUser.count()
    println(drr)
    sc.stop()
  }
}
