package com.ehl.offline.business

import com.ehl.offline.common.{EhlConfiguration, ObjectUtils}
import com.ehl.offline.time.strategy.TimeLine
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2016/12/8.
  */

// 一次出行(trip) 序列元素
case class TripRecord(
                      passtime: Long,
                      cid: Short
                     ){
  override def toString=passtime+"-"+cid
}

object VehicleTripFunction {
  private val vehicleTripSingle = new VehicleTripFunction();

  /**
    * 生成过车轨迹过滤条件{
    * }
    *  //保存格式  numb-plate_type,time-cid`time-cid,time-cid`time-cid
    * @param rdd
    * @param path
    */
  def generatedTripTracker(rdd:RDD[(String,Iterable[Tracker])],path:String,timeLine:TimeLine)={
    rdd.filter(f=>f._2.size>1).mapPartitions(iter=>{
      iter.map(inner=>{
        //记录一次完整轨迹
        val array = collection.mutable.ListBuffer[TripRecord]()
        val setpTripArray = collection.mutable.ArrayBuffer[String]()
        val numb_plateType = inner._1
        val innerValues=inner._2.toList.sortBy(value=>value.passTime)
        //遍历
        for(_i <- 0 to innerValues.size-2){
          val first =  innerValues(_i)
          val second = innerValues(_i+1)
          if(array.size==0) {
            array += TripRecord(first.passTime, first.cid);
          }

          //TODO add time strategy
          val times = vehicleTripSingle.timeHour(first.passTime,second.passTime)
          val cards = first.cid+"-"+second.cid+"`"+times._1+"`"+times._2
          val realTime = timeLine.getTime(cards)
//          println(realTime+"-----------------------------"+cards)
          if(second.passTime - first.passTime >realTime){
            if(array.size>1) {setpTripArray += array.mkString("`")}
            array.clear()
            array += TripRecord(second.passTime,second.cid)
          }else{
            val span = (second.passTime -first.passTime)
            if ((span > timeLine.minTime)) {
              array +=TripRecord(second.passTime,second.cid)
            }
          }

          if(_i == innerValues.size-2){
            array.clear()
          }
        }
        (numb_plateType,setpTripArray.mkString(","))
      })
    }).filter(f=>ObjectUtils.noEqual(f._2,"")).map(f=>f._1+","+f._2).repartition(10).saveAsTextFile(path)
  }

}

class VehicleTripFunction{
  val MAX_INTERVAL = 30 * 60 * 1000 // 两条过车记录最大间隔时间
  val MIN_TRAVELTIME = 3.0d
  val MAX_TRAVELTIME = 30 * 60d
 //TODO get config

  def timeHour(time1:Long,time2:Long):(String,String)={
    val date1 = new DateTime(time1)
    val date2 = new DateTime(time2)

    val hour1 = date1.getHourOfDay
    val min1 = date1.getMinuteOfHour

    val start_1 = innerTime(hour1,min1)

    val hour2 = date2.getHourOfDay()
    val min2 = date2.getMinuteOfHour
    val end_1 = innerTime(hour2,min2)
    val a=start_1._1+":"+start_1._2
    if(start_1._1 == end_1._1 && start_1._2.equals(end_1._2)){

      val m = if(start_1._2.equals("00")) start_1._1+":30" else (hour1+1)+":00"
      (a,m)
    }else{
      (a,end_1._1+":"+end_1._2)
    }
  }

  def innerTime(hour:Int,min:Int):(Int,String)={

    if(min <=20 ) (hour,"00")
    else if(min <=40) {(hour,"30")}
    else {(hour+1,"00")}
  }

}
