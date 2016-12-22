package com.ehl.offline.business

import com.ehl.offline.common.ObjectUtils
import org.apache.spark.rdd.RDD

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
  def generatedTripTracker(rdd:RDD[(String,Iterable[Tracker])],path:String)={
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
          if(second.passTime - first.passTime >vehicleTripSingle.MAX_INTERVAL){
            if(array.size>1) {setpTripArray += array.mkString("`")}
            array.clear()
            array += TripRecord(second.passTime,second.cid)
          }else{
            val span = (second.passTime -first.passTime) / 1000
            if ((span > vehicleTripSingle.MIN_TRAVELTIME) && (span < vehicleTripSingle.MAX_TRAVELTIME)) {
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
}
