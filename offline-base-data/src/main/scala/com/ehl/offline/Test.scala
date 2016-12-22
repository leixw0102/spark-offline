package com.ehl.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.ehl.offline.business.Tracker
import org.apache.hadoop.io.{LongWritable, Text}
//import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession

/**
  * Created by 雷晓武 on 2016/12/7.
  */
object Test extends App{

  //first

  val conf=new SparkConf().setAppName("test")//.setMaster("local[*]")
//  val session= SparkSession.builder().config(conf).getOrCreate()
  val sc = new SparkContext(conf)
  try{
    //保存hadoop

//    val sc = session.sparkContext
//    import session.implicits._//    session.read.text()
    val lines = sc.textFile(args(0)).map(f=>f.split(",",17))//args(0)).map(f=>f.split(",",17))
      .filter(f=>f.length==17)
      .filter(f=>noEqual(f(3).split("=")(1),"无牌"))
      .filter(f=>noEqual(f(3).split("=")(1),"00000000"))
      .filter(f=>f(3).split("=")(1) != None)
      .map(f=>{
//      val timeString = (f(1).split("="))(1)
//        val formaterFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS")
//      val passtime = formaterFull.parse(timeString).getTime
//        (
//        (f(3).split("=")(1)),
//        (f(15).split("=")(1)),//.toShort,
//        f(5).split("=")(1),//.toDouble,
//        (f(10).split("=")(1)),//.toInt,
//        f(9).split("=")(1))//.toInt)
    })
    lines.count()
    lines.repartition(2).saveAsTextFile("/app/v1")
//    lines.printSchema()
//    lines.show(10,false)
  }catch{
    case ex:Exception=>ex.printStackTrace()
  } finally{
    //end
    sc.stop()
  }
  def noEqual(first:String,second:String):Boolean={
    !first.equals(second)
  }
}
