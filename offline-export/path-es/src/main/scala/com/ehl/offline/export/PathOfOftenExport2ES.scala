package com.ehl.offline.export

import java.util.Date

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkWithEhl
import com.ehl.offline.es.ESConfConstant
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
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
    val fromSystem = System.getProperty("es_conf")
    val conf = if(fromSystem ==null || fromSystem.isEmpty) "es.conf" else fromSystem
    new EhlConfiguration().addResource(conf)
  }

  operateSpark(args,ehlConf)(op=>{

    val sql = new SQLContext(op)
    import sql.implicits._
//    sql.read.parquet(args(0))
    val load = sql.read
      .parquet(args(0))
      load.map(f=>{
        val numb_plate = f.getString(0).split("-")
        PathOfOften(numb_plate(0),numb_plate(1).toInt,f.getString(1),f.getString(2),f.getString(3),f.getInt(4))
      }).toDF().saveToEs(ehlConf.get(exportPathIndexType)+new DateTime().plusDays(-1).toString("yyyy-MM-dd"))

  })
}
