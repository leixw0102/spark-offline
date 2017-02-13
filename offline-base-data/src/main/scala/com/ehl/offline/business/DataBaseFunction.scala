package com.ehl.offline.business

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2016/12/6.
  */
case class BayPair(start: Short, end: Short, // 起始卡点，结束卡点
                   startTime: Long, endTime: Long, // 经过卡点时间
                   timestamp: Long){

  val start_msg=(start,startTime)
  val end_msg=(end,endTime)

  val bayPair=(start,end)

  val bayPair_timePair=(startTime,endTime)

  override def toString=startTime+","+endTime+","+timestamp

}// 过车时间{


// DRIVEWAY 车道编号  drivedir 行驶方向
case class Tracker(passTime:Long,day:String,numb:String,plateType:Int,cid:Short,speed:Double,drivedir:Int,driveway:Int){
  override def toString={
    day+"\t"+passTime+"\t"+numb+"\t"+cid+"\t"+speed+"\t"+drivedir+"\t"+driveway
  }
}

/**
  * 卡点对逻辑
  */
class DataBaseFunction extends Serializable{


  /**
    * 求中位数
    *
    * @param arr
    * @return
    */

  def median(arr: List[Long]) = {
    if (arr.size == 1)
      arr(0)
    else {
      val sorted = arr.sortBy(x => x)
      val indx = (sorted.size - 1) / 2

      if (arr.size % 2 > 0) sorted(indx) else (sorted(indx) + sorted(indx + 1)) / 2
    }
  }


  /**
    * 排除异常旅行时间记录
    *
    * @param iter
    * @return
    */
  def removeOutliers(iter: Iterable[BayPair]): Iterable[BayPair] = {
    val arr = iter.toList
    val thresh = 2.8
    if (arr.size < 3)
      arr
    else {
      // find median
      val med = median(arr.map(x => {
        x.timestamp
      }))

      val disp = arr.map(x => Math.abs(x.timestamp - med))
      val mdev = median(disp)
      val ratio = disp.map(x => {
        if (mdev > 0) (x / mdev) else 0
      })

      arr.zip(ratio).filter(x => x._2 < thresh).map(x => x._1)
    }
  }
  val granula = 1 * 60 * 60 * 1000 // 粒度1小时
  /**
    * 根据时间过滤
    *
    * @param vs
    * @return
    */
  def filterBayPairTime(vs: Iterable[BayPair]):Iterable[Long]={
    vs
//      .filter(x=>{
//      val curtime = x.endTime
//      val bound = curtime / granula
//      val time = new DateTime(bound * granula).toString("HH").toInt
//      (time>=PairConf.getStartH && time<=PairConf.getEndH)
//    })
      .filter(p=>p.timestamp != 0)
      .map(data=>data.timestamp)
  }

  def getCollectionFirstLong(collection:Iterable[Long]):Long={
    if(collection.isEmpty || collection.size==0) 0L else  collection.head
  }
  def getCollectionFirstInt(collection:Iterable[Int]):Int={
    if(collection.isEmpty || collection.size==0) 0 else  collection.head
  }

  /**
    * (a,b),5
    *
    * return
    * (short,short),int,long
    *
    * @param pair
    */
  private def countBayPair(pair:RDD[((Short,Short),Iterable[BayPair])])={
    pair.mapValues(f=>f.mkString("`"))
//    val pair_size=pair.map(f=>(f._1,f._2.size)).sortBy(f=>f._2,false)
//
//    val pair_times=pair.map(f=>{
//      (f._1,filterBayPairTime(f._2))
//    }).filter(f=>f._2.size !=0 )
//      .map(f=>(f._1,f._2.sum/f._2.size))
//    pair_size.cogroup(pair_times).map(f=>(f._1,getCollectionFirstInt(f._2._1),getCollectionFirstLong(f._2._2)))
  }






}

object DataBaseFunction {
  val single = new DataBaseFunction()
  /**
    *
    * //TODO 根据轨迹找到
    * 对数据进行生成
    *
    * @param q
    * @return
    */
  def convertToPair(q: Iterable[Tracker]):Iterator[BayPair] = {

    q.toList.sortBy { x => x.passTime }.sliding(2,1)
      //去掉条件过滤，后续用的时候在去过滤，用于找到卡点之间正常的通过时间
//      .filter(pairlist=> {
//      pairlist.head.cid != pairlist.last.cid &&(pairlist.last.passTime - pairlist.head.passTime) / 60000 < 30 && (pairlist.last.passTime - pairlist.head.passTime) / 1000 > 3
//    })
      .map(x=>BayPair(x.head.cid,x.last.cid,x.head.passTime,x.last.passTime,x.last.passTime-x.head.passTime))
  }
  //conf  获取配置文件
  /**
    * 对数据进行ETL过滤
    *
    * @param rdd
    * @return
    */
  def generatedPairAndRemoveDirtyData(rdd:RDD[(String,Iterable[Tracker])])={
    rdd.mapValues(f=>{convertToPair(f)})
      .flatMap(f=>f._2  ).groupBy(_.bayPair).mapPartitions(iter => {
      iter.map(x => {
        (x._1,single.removeOutliers(x._2))
      })
    })
  }
  /**
    *卡点对逻辑处理
    * 格式 short-short,int,long
    *
    * @param pair
    * @param path
    */
  def pairAndSave(pair:RDD[((Short,Short),Iterable[BayPair])],path:String)={

    val pairResult = single.countBayPair(pair)
    //保存卡点对 格式 short-short,int,long
    pairResult.map(f=>f._1._1+"-"+f._1._2+","+f._2).repartition(5).saveAsTextFile(path)
  }



  /**
    * 过车记录进行转换
    *
    * @param f
    * @return
    */
  def convertToTracker(f:Array[String]):Tracker={
      val timeString = (f(1).split("="))(1)
      val passtime = toDate(timeString,f)
      val t=Tracker(passtime._1,
        passtime._2,
        f(3).split("=")(1),
        toInt(f(4).split("=")(1),f),
        toShort(f(15).split("=")(1),f),
        toDouble(f(5).split("=")(1),f),
        toInt(f(10).split("=")(1),f),
        toInt(f(9).split("=")(1),f))
    t

  }

  private def toDate(date:String,f:Array[String]):(Long,String) = {
    try {
      val formaterFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")
      (formaterFull.parse(date).getTime,date.substring(0,10))
//      if(date.length<10)
    }catch {
      case ex:Exception=>{
        println(f.mkString(","))
        val d=new Date()
        (d.getTime,new DateTime(d).toString("yyyy-MM-dd"))
      }
    }
  }

  private def toDouble(input:String,f:Array[String])={
    try{
      input.toDouble
    }catch {

      case e:Exception=>{
        println(f.mkString(","))
        0.0
      }
    }
  }

  private def toInt(input:String,f:Array[String])={
    try{
      input.toInt
    }catch {
      case e:Exception=>{
        println(f.mkString(","))
        0
      }
    }
  }

  private def toShort(input:String,f:Array[String])={
    try{
      input.toShort
    }catch {
      case e:Exception=>{
        println(f.mkString(","))
        0.toShort
      }
    }
  }
}
