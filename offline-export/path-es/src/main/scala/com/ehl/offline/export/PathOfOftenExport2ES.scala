package com.ehl.offline.export

import java.util.Date

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkWithEhl
import com.ehl.offline.es.ESConfConstant
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
import scala.collection.JavaConversions._
/**
  * Created by 雷晓武 on 2016/12/21.
  */
case class PathOfOften(numb:String,plate_type:Int,cids:String,start:String,end:String,num:Int)
object PathOfOftenExport2ES extends AbstractSparkWithEhl with ESConfConstant{
  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "export data of often path to es "+new Date().toString

  override def initEhlConfig: EhlConfiguration = {
    val fromSystem = System.getProperty("es-conf","es.conf")

    val conf = new EhlConfiguration().addResource(fromSystem)
    conf.foreach()
    conf
  }

  operateSpark(args,ehlConf)(op=>{
    val sql = new SQLContext(op)

    import sql.implicits._
//    sql.read.parquet(args(0))
    val load = sql.read
      .parquet(args(0))
      .map(f=>{
        val numb_plate = f.getString(0).split("-")
        PathOfOften(numb_plate(0),numb_plate(1).toInt,f.getString(1),f.getString(2),f.getString(3),f.getInt(4))
      })
  //TODO add top5
//    .groupBy(f=>f.numb+"-"+f.plate_type).mapValues(values=>values.toSeq.sortWith((f1,f2)=>f1.num>f2.num).take(5))
//    .flatMap(f=>f._2)
    .toDF()
      load.schema
      load.show(10,false)
//        load.saveToEs()
//    println(ehlConf.get(exportPathIndexType))
        load.saveToEs(ehlConf.get(exportPathIndexType))

  })
}
