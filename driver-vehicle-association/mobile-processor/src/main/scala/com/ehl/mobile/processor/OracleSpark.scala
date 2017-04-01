package com.ehl.mobile.processor

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.MessageFormat
import java.util.{Date}

import com.ehl.mobile.utils.UUIDUtil
import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime



//case class Tracker(number_type:String,cid:Long,time:Array[Long])

/**
  * Created by 雷晓武 on 2017/2/14.
  */
object OracleSpark extends AbstractSparkEhl  with App{

  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "mobile match with 1 "

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource(System.getProperty("mv","mv.conf"));
  }

//  def saveToDb(df:DataFrame,sql:String,date:Date,conf:EhlConfiguration)={
//    df.map(f=>{
//      val num_type = f.getString(0).split("-")
//      val num = num_type(0)
//      val t = num_type(1)
//      val ps = f.getString(1).split(",")
//      ps.map(data=>{
//        val p_c = data.split("-")
//        Row(num,t,p_c(0),p_c(1).toInt,new Timestamp(date.getTime))
//      })
//    }).flatMap(f=>f)
//      .foreachPartition(insertDataFunc(_,sql,conf))
//  }


  def saveToDb(df:DataFrame,sql:String,date:Date,conf:EhlConfiguration)={
    df.map(f=> {
//      val num_type = f.getString(0).split("-")
//      val num = num_type(0)
//      val t = num_type(1)
//      Row(num, t, f.getString(1), f.getInt(2), new Timestamp(date.getTime))
      Row(f.getString(0),f.getString(1),f.getString(2),f.getInt(3),new Timestamp(date.getTime))
    }).foreachPartition(insertDataFunc(_,sql,conf))
  }

  operateSpark(args ,ehlConf)(op=> {

    val date = DateTime.now()
      val yesterday= date.plusDays(-1).toString("yyyy-MM-dd")
    val imsiSql="INSERT INTO T_ITGS_IMSI_MAPPING_NEW(BH,HPHM,HPZL,IMSI,MATCHNUM,UPDATETIME) VALUES ( ?,?,?,?,?,?)"
//    val imeiSql="INSERT INTO T_ITGS_EMSI_MAPPING(BH,HPHM,HPZL,EMSI,MATCHNUM,UPDATETIME) VALUES ( ?,?,?,?,?,?)"
    val session = new SQLContext(op)
    import session.implicits._
    args(0) match {
      case "ALL" =>{
        saveToDb(session.read.parquet(MessageFormat.format(ehlConf.get("result.hdfs.path"),yesterday)),imsiSql,date.toDate,ehlConf)
//        saveToDb(session.read.parquet(MessageFormat.format(ehlConf.get("hdfs.imei.path"),yesterday)),imeiSql,date.toDate,ehlConf)
      }
      case "IMSI" =>{
        saveToDb(session.read.parquet(MessageFormat.format(ehlConf.get("hdfs.imsi.path"),yesterday)),imsiSql,date.toDate,ehlConf)
      }
      case "IMEI" =>{
//        saveToDb(session.read.parquet(MessageFormat.format(ehlConf.get("hdfs.imei.path"),yesterday)),imeiSql,date.toDate,ehlConf)
      }
      case _=>None
    }

    //
//    val shareCards = readFile


//    saveToDb(df)
  })


val batchSize=100


//  jdbcDF.foreachPartition(insertDataFunc)
  def insertDataFunc(iterator: Iterator[Row],insertSql:String,conf:EhlConfiguration): Unit = {
  var conn: Connection = null
  var psmt: PreparedStatement = null

  var i = 0
  var num = 0
  try {
    Class.forName(conf.get("db.default.driver")).newInstance()
    conn = DriverManager.getConnection(conf.get("db.default.url"), conf.get("db.default.user"), conf.get("db.default.password"))
    conn.setAutoCommit(false);
    psmt = conn.prepareStatement(insertSql)
    iterator.foreach { row =>
    {
      i += 1
      if (i > batchSize) {
        i = 0
        psmt.executeBatch();
        num += psmt.getUpdateCount();
        psmt.clearBatch();
      }
      psmt.setString(1,UUIDUtil.generateShortUuid())
      psmt.setString(2, row.getString(0))
      psmt.setString(3, row.getString(1))
      psmt.setString(4, row.getString(2))
      psmt.setInt(5,row.getInt(3))
      psmt.setTimestamp(6,row.getTimestamp(4))
      psmt.addBatch();
    }
    }
    psmt.executeBatch();
    num += psmt.getUpdateCount();
    conn.commit();
    println(num+"..........................")
  } catch {
    case e: Exception => {
      e.printStackTrace()
      try {
        conn.rollback();
      } catch {
        case e: Exception => e.printStackTrace();
      }
    }
  } finally {
    if (psmt != null) {
      psmt.close()
    }
    if (conn != null) {
      conn.close()
    }
  }
  }

}
