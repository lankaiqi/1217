package day0613

import org.apache.commons.lang3.time.FastDateFormat


/**
  * Created by Administrator on 2018/6/13.
  */
object FilterUtils {

  //val sdf = new SimpleDateFormat("")
  val dataFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")


  def filtetByTime(fields:Array[String] ,startTime:Long , endTime: Long): Boolean  = {
    val time = fields(1)
    val logTime = dataFormat.parse(time).getTime
    logTime>=startTime && logTime < endTime
  }

  def filterByType(fields:Array[String] , evenType :String): Boolean={
    val _type = fields(0)
    evenType == _type
  }

  def filterByTypes(fields:Array[String] ,eventTypes: String* ):Boolean={
    val _type = fields(0)
    for (et <-eventTypes){
      if (et == _type)  return true
    }
    false
  }

  def filterByTypeAndTime(filters : Array[String] , eventType: String , beginTime :Long ,endTime :Long):Boolean={
    val _type = filters(0)
    val _time = filters(1)
    val longTime = dataFormat.parse(_time).getTime
    eventType==_type && longTime>= beginTime && longTime < endTime
  }





}
