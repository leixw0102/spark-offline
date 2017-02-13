package com.ehl.offline.time.strategy

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2017/1/16.
  */
abstract class TimeLine(ehl:EhlConfiguration) extends Serializable{
   def init:RDD[(String,Long)]
    def minTime:Long = ehl.getLong("time.min.time",3)*1000
   def maxTime:Long=ehl.getLong("time.max.time",3600)*1000
   def getTime(key:String):Long
}

class AvgTimeLine(ehl:EhlConfiguration, cacheRdd:Broadcast[scala.collection.Map[String,Long]]) extends TimeLine(ehl){

  val cache = cacheRdd.value

  override  def getTime(key: String): Long = {

    if(cache.contains(key)){
      cache.getOrElse(key,maxTime)
    }else{
       maxTime
    }
  }

  override def init: RDD[(String, Long)] = throw new Exception("")
}

class DefaultTimeLine(ehl:EhlConfiguration) extends TimeLine(ehl){

  override  def getTime(key: String): Long = ehl.getLong("time.strategy.time.default",1800)*1000

  override def init: RDD[(String, Long)] = throw new Exception("")
}

object TimeStrategy extends Enumeration{
  type TimeStrategy = Value
  val CONSTANT,AVG=Value
}
object TimeLine{
  def getClass(conf:EhlConfiguration,broadcast: Broadcast[scala.collection.Map[String,Long]]):TimeLine={
    val timeType = conf.get("time.strategy.default","CONSTANT")
    TimeStrategy.withName(timeType.toUpperCase) match {
      case TimeStrategy.AVG =>{
        new AvgTimeLine(conf,broadcast)
      }
      case TimeStrategy.CONSTANT=>new DefaultTimeLine(conf)
    }
  }
}
