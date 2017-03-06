package com.ehl.mobile.function

import com.ehl.mobile.processor.MobileBaseData2
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2017/3/2.
  */
class VMMatchEveryDay(val shareTracker:Array[Row],val mobile:RDD[MobileBaseData2],val floatingSeconds:Int=120) extends Matched[RDD[(String,Long,String,String)]] with Serializable{
  override def matchAnalyze :RDD[(String,Long,String,String)]= {
    val mapTracker = shareTracker.map(f=>(f.getLong(3),(f.getString(1)+"-"+f.getInt(2),f.getLong(0))))
      .groupBy(f=>f._1)
    //timestamp:Long,imsi:String,imei:String,baseStationNumber:Long
    mobile.map(f => {
      val cid = f.baseStationNumber
      mapTracker.get(cid).map(array=>{
        array.map(a=>{
          (a._2._1,cid,a._2._2,f.timestamp,f.imsi,f.imei)
        })
      })
      //numb_type,cid,ts,ts1,imsi,imei

    }).flatMap(f=>f).flatMap(f=>f)
      .filter(f=>{
        val ts = f._3
        val currentTime = new DateTime(ts)
        val timestamp = f._4.toLong
        timestamp>=currentTime.plusSeconds(-floatingSeconds).toDate.getTime && timestamp <= currentTime.plusSeconds(floatingSeconds).toDate.getTime
      })
      .map(f=>(f._1,f._2,f._5,f._6))
  }

//  def test={
//    matchAnalyze
//  }
}
