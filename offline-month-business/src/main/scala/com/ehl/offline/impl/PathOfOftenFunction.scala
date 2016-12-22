package com.ehl.offline.impl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext}
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2016/12/14.
  */
case class PathOfOftenBase(numb_plateType:String,cids:String,start:String,end:String){
  override def toString=numb_plateType+","+cids+","+start+","+end
//  val groupByKey = (numb_plateType,cids)
}

case class PathOfOftenBaseNumber(numb_plateType:String,cids:String,start:String,end:String,num:Int){
  val defaultGroupKey = numb_plateType

  val defaultSortKey = num

  override def toString=numb_plateType+","+cids+","+start+","+end+","+num
}


object PathOfOftenFunction {
  val pathOfOftenFunction = new PathOfOftenFunction()

  def executePathOfOftenProcesser(op:SQLContext, dataSets:RDD[String], output:String):Unit={
      val result = pathOfOftenFunction.transformAndAction(op,dataSets)
//      result.show(30,false)
      result.repartition(10).write.parquet(output)
    }
}

class PathOfOftenFunction extends Serializable{
//  def
  /**
    * 算法实现
    * trip  与时间关系：同一个trip 时间可能不同
    * 对时间以半小时为粒度进行取整
    * @param op
    * @param dataSets
    * @return
    */
  def transformAndAction(op:SQLContext, dataSets:RDD[String])={
    import op.implicits._
    //      dataSets
    dataSets.map(f=>{
      val splits = f.split(",")
      val numb_plateType =splits(0)
      val time_cid_array = splits.drop(1)
      val array = scala.collection.mutable.ArrayBuffer[PathOfOftenBase]()
      for (time_cid<-time_cid_array){
        val cids = for(temp<-time_cid.split("`");cid = temp.split("-")(1)) yield{ cid}
        //TODO add start of trip and end of trip
        val tcs = time_cid.split("`")
        val startTime = getStartTimePathOfOftenForTimeWithThreshold((tcs.head.split("-")(0)).toLong)
        val endTime = getEndTimePathOfOftenForTimeWithThreshold((tcs.last.split("-")(0)).toLong)
        array +=PathOfOftenBase(numb_plateType,cids.mkString("-"),startTime,endTime)
      }
      array
    }) //加上时间进行分组
      .flatMap(f=>f)
      .map(f=>(f,1))
      .reduceByKey(_+_)
      .map(f=>PathOfOftenBaseNumber(f._1.numb_plateType,f._1.cids,f._1.start,f._1.end,f._2))
      .groupBy(f=>f.defaultGroupKey).mapValues(values=>values.toSeq.sortWith((f1,f2)=>f1.num>f2.num).take(5))
          .flatMap(f=>f._2).toDF()

//      .toDS()
//      .groupBy(f=>f)
//      .mapGroups((k,vs)=>{
//      PathOfOftenBaseNumber(k,vs.length)
//    }).groupBy(f=>f.defaultGroupKey).mapGroups((path,values)=>{
//      values.toSeq.sortBy(_.defaultSortKey)(Ordering[Int].reverse).take(5)
//    })


    //spark 2.0.2
//      .groupByKey(f=>f.groupByKey).mapGroups((k,vs)=>{
//      //TODO find the min and max of the trips
//      k._1+","+k._2 +","+getMinLong(vs)+","+getMaxLong(vs)+","+vs.length
//    })


    //version
//      .toDS().groupByKey(f=>f.numb_plateType).count().map(f=>{
//      val numb_trip=f._1.split(",")
//      (numb_trip(0),numb_trip(1)+"`"+f._2)
//    }).groupByKey(f=>f._1).mapGroups((k,vs)=>{
//      val trip_num = vs.map(f=>f._2).mkString(",")
//
//      k+","+trip_num
//
//    })//.show(30,false)
  }

  val timeThreshold=30

  def getStartTimePathOfOftenForTimeWithThreshold(time:Long): String ={
    val dateTime = new DateTime(time)
    val mintue = dateTime.getMinuteOfHour

    val expectedHourAndMinute = if(mintue >timeThreshold) dateTime.getHourOfDay+":30:00" else dateTime.getHourOfDay+":00:00"
    expectedHourAndMinute
  }

  def getEndTimePathOfOftenForTimeWithThreshold(time:Long): String ={
    val dateTime = new DateTime(time)
    val mintue = dateTime.getMinuteOfHour

    val expectedHourAndMinute = if(mintue >timeThreshold) dateTime.getHourOfDay+1+":00:00" else dateTime.getHourOfDay+":30:00"
    expectedHourAndMinute
  }

}
