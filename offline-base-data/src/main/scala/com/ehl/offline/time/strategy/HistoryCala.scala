package com.ehl.offline.time.strategy

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/***
  * Created by 雷晓武 on 2017/1/19.
  */
object HistoryCala {
  val singleton = new HistoryCala()


  def timeProcesserAndSave(rdd:RDD[CardBase], filter:Long, downFloor:Double,upFloor:Double,path:String)={
    rdd.filter(f=>f.time<=filter)
      .map(singleton.timeProcesser(_))
             .map(f=>(f.key,f.value))
          .groupByKey()
            .mapValues(f=>
            {
              val size = f.size
              val downSize = Math.floor(size * downFloor)
              if(size == downSize){
                Math.floor(f.toList.sorted.last*(1+upFloor)).toLong
              }else {
                val head = f
                  .toList
                  .sortWith((l1,l2)=>l1>l2)
                  .drop(downSize.toInt)
                  .head
                Math.floor(head*(1+upFloor)).toLong
              }
            })
        .filter(f=>f._2>0)
        .map(f=>f._1+"\t"+f._2).saveAsTextFile(path)
//      .reduceByKey((f1,f2)=>{
//        val times = f1._1+f2._1
//        val count = f1._2+f2._2
//        (times,count)
//      })
//      .map(f=>{
//      val time_count = f._2
//      val avg = Math.round(time_count._1/time_count._2)
//      f._1+"\t"+avg
//    })
//      .saveAsTextFile(path)

  }
}
case class tempCardBase(card:String,start:String,end:String,time:Long){
  val key = card+"`"+start+"`"+end
  val value = time
}
class HistoryCala{

  def timeProcesser(card:CardBase):tempCardBase={
    val start_end = timeHour(card.start,card.end)

    tempCardBase(card.cards,start_end._1,start_end._2,card.time)
  }

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
