package com.ehl.offline.impl

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

import scala.collection.mutable.ArrayBuffer

case class CrossCard(startTime:Long,endTime:Long,interval:Long)

case class TimeCorssCard(start:Int,end:Int,times:Long)

/**
  * Created by 雷晓武 on 2017/1/11.
  */
class CrossCardTimeFunction {

    def convert2CrossCard(line:String)={
      val firstCommaPos=line.indexOf(",")
      val cards = line.take(firstCommaPos)
      val other = line.drop(firstCommaPos+1).split("`").toList
      val array = scala.collection.mutable.ArrayBuffer[CrossCard]()
      for(temp <- other){
        val fileds = temp.split(",")
        array+=CrossCard(fileds(0).toLong,fileds(1).toLong,fileds(2).toLong)
      }
      (cards,array)
    }

    def calculateCrossTime(crossCards :Iterable[(String,ArrayBuffer[CrossCard])],maxTime:Long):Iterable[(Int,Int,Long)]={

      crossCards
          .flatMap(_._2)
          .filter( _.interval <= maxTime)
            .map(f=>{
              val start = getHalfHour(f.startTime)
              val tempEnd =getHalfHour(f.endTime)
              val end = if(start==tempEnd) tempEnd+1 else tempEnd
              (start,end,f.interval)
            })
    }

  def getHalfHour(time:Long): Int ={
    val dateTime = new DateTime(time)
    val mintue = dateTime.getMinuteOfHour

    if(mintue >halfHour) dateTime.getHourOfDay+1 else dateTime.getHourOfDay
  }


  def getTimeOfHour(rdd:Iterable[(String,Long)],remove:Double,up:Double): (String,Long) ={
    val tempRdd = rdd.toSeq.sortWith((f1,f2)=>f1._2>f2._2)
      val otherList=tempRdd.drop(Math.ceil(tempRdd.size*remove).toInt)
      val tempResult = if(otherList.size>0) otherList.head else tempRdd.head
    (tempResult._1,Math.ceil(tempResult._2*up).toLong)
  }

  val halfHour=30;
}
object CrossCardTimeFunction{
  val singleton = new CrossCardTimeFunction

  def crossCardTime(rdd:RDD[String],maxTime:Long,removeRate:Double,upRate:Double,savePath:String)={
    rdd
      .map(singleton.convert2CrossCard(_))
        .groupBy(_._1).mapValues(singleton.calculateCrossTime(_,maxTime))
           .map(record=>{
             val cards = record._1
             record._2.map(time=>(cards+"-"+time._1+"-"+time._2,time._3))
           })
      .flatMap(f=>f)
      .groupBy(f=>f._1)
        .filter(f=>f._2.size!=0)
        .mapValues(singleton.getTimeOfHour(_,removeRate,upRate))
      .map(_._2).map(f=>f._1+","+f._2).repartition(10).saveAsTextFile(savePath)
  }

}
