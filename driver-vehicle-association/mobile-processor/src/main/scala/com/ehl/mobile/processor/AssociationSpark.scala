package com.ehl.mobile.processor

import java.text.{MessageFormat, SimpleDateFormat}
import java.util.Date

import org.apache.hadoop.io.compress.SnappyCodec
import com.ehl.offline.common.{EhlConfiguration, ObjectUtils}
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime
case class MobileBaseData2(timestamp:Long,imsi:String,imei:String,baseStationNumber:Long)
case class Tracker2(passTime:Long,numb:String,plateType:Int,cid:Long){
  override def toString={
    passTime+"\t"+numb+"\t"+cid+"\t"
  }
}
/**
  * Created by 雷晓武 on 2017/2/27.
  */
object AssociationSpark extends AbstractSparkEhl with EhlInputConfForHdfsConf  with App{
  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "m-v association "

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource(System.getProperty("mv","mv.conf"))
  }
  System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");
  operateSpark(args ,ehlConf)(sc=>{
    sc.hadoopConfiguration.addResource("hdfs-site.xml")
    println(sc.hadoopConfiguration.get("dfs.nameservices")+"\t"+sc.hadoopConfiguration.get("fs.defaultFS")+"\t"+sc.hadoopConfiguration.get("ha.zookeeper.quorum") +"\t"+sc.hadoopConfiguration.get("dfs.namenode.rpc-address.cluster1.nn1"))
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val share=readFile
    val shareCardsSet=share.values.map(f=>f.toLong).toSet

    val inputDate=args(0)
    val vPath=MessageFormat.format(ehlConf.get("v.hdfs.path"),inputDate)
    println("vechile path ="+vPath)
    val vechile =sc.textFile(vPath)
      .map(f => f.split("\t",17)) //args(0)).map(f=>f.split(",",17))
      .filter(f => f.length == 17)
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), "无牌"))
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), "00000000"))
      .filter(f => f(3).split("=")(1) != None)
      .map(f => convertToTracker(f))
      .filter(f=>shareCardsSet.contains(f.cid))
      .toDF()
//    vechile.show(20,false)
    val mPath=MessageFormat.format(ehlConf.get("mobile.hdfs.path"),inputDate.replaceAll("-",""))
    println("mobile path ="+mPath)
    val mobileDataRdd=sc.wholeTextFiles(mPath,15)
      .map(f=>f._2).map(f=>f.split("\n")).flatMap(f=>f).map(data=>{
      val spliter = data.split(",",25)
      MobileBaseData2(spliter(0).toLong*1000,spliter(6),spliter(7),spliter(11).toLong)
    })
      .filter(f=>share.contains(f.baseStationNumber+"")).map(f=>MobileBaseData2(f.timestamp,f.imsi,f.imei,share.getOrElse(f.baseStationNumber+"","0").toLong))
//      .toDF()
//    println(mobileDataRdd.count())
    val result = matchMV(vechile.collect(),mobileDataRdd,ehlConf.getInt("mobile.time.between.float",120))
//    result.saveAsTextFile(MessageFormat.format(ehlConf.get("mv.association.hdfs.save.path"),inputDate))
//    result.saveAsTextFile(MessageFormat.format(ehlConf.get("mv.association.hdfs.save.path"),inputDate))
      .toDF("num_type","cid","imsi","imei")
//      println(result.count)
      .write.option("spark.sql.parquet.compression.codec","snappy")
      .parquet(MessageFormat.format(ehlConf.get("mv.association.hdfs.save.path"),inputDate))

    //numb_type,cid,imsi,imei

  })


  def convertToTracker(f:Array[String]):Tracker2={
    val timeString = (f(1).split("="))(1)
    val passtime = toDate(timeString,f)
    val t=Tracker2(passtime._1,
      f(3).split("=")(1),
      toInt(f(4).split("=")(1),f),
      toLong(f(15).split("=")(1),f))

    t

  }

  /**
    * from  Tracker2(passTime:Long,numb:String,plateType:Int,cid:Long){
    * return //numb_type,cid,imsi,imei
    *
    * @param shareTracker
    * @param mobile
    * @param floatingSeconds
    * @return
    */
  def matchMV(shareTracker:Array[Row],mobile:RDD[MobileBaseData2],floatingSeconds:Int=180) ={
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
//      .map(f=>f._1+","+f._2+""+f._5+""+f._2)
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
  private def toLong(input:String,f:Array[String])={
    try{
      input.toLong
    }catch {
      case e:Exception=>{
        println(f.mkString(","))
        0.toLong
      }
    }
  }


  def readFile:Map[String,String]={
    val dictionaryFile = System.getProperty("base_dictionary","base_dictionary.dat")
    val conf = new EhlConfiguration().addResource(dictionaryFile)
    conf.toMap()
  }

  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    Array()
  }
}
