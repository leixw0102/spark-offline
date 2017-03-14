package com.ehl.mobile.processor

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.MessageFormat
import java.util.Date

import com.ehl.mobile.utils.UUIDUtil
import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.{AbstractSparkEhl}
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 雷晓武 on 2017/2/27.
  */
object AssociationMonthSpark extends AbstractSparkEhl with EhlInputConfForHdfsConf with App{
  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
    val path_prefix=conf.get("month.hdfs.path")
    //    Array(conf.get(mobilePath))
    val from =conf.get("month.hdfs.from")
    val realFrom = if(from.isEmpty || from == null) DateTime.now().plusDays(-1).toString("yyyy-MM-dd") else from
    val size = conf.getInt("month.hdfs.size",120)
    //
    val fromDate = DateTime.parse(realFrom,formater)
    val array = scala.collection.mutable.ArrayBuffer[String]()
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsConf)
    for(i<- 0 to size){
      val temp =path_prefix+File.separator+fromDate.plusDays(-i).toString("yyyy-MM-dd");
      println(temp)
      if(exitDirectoryWithHadoop(temp,fs)) array+=(temp)
    }
    fs.close()
    array.toArray
  }

  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "month association m-v"

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource("mv.conf")
  }

  def deleteDb(sql: String,conf:EhlConfiguration) = {
    var conn: Connection = null
    var psmt: PreparedStatement = null
    try {
      Class.forName(conf.get("db.default.driver")).newInstance()
      conn = DriverManager.getConnection(conf.get("db.default.url"), conf.get("db.default.user"), conf.get("db.default.password"))
      psmt = conn.prepareStatement(sql)
      psmt.executeUpdate()

    } catch {
      case e: Exception => {
        e.printStackTrace()

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

  operateSpark(args,ehlConf)(op=>{
    println(op.hadoopConfiguration.get("dfs.nameservices")+"\t"+op.hadoopConfiguration.get("fs.defaultFS")+"\t"+op.hadoopConfiguration.get("ha.zookeeper.quorum") +"\t"+op.hadoopConfiguration.get("dfs.namenode.rpc-address.cluster1.nn1"))
    val currentDate = DateTime.now()
    val sqlContext  = new SQLContext(op)
    import sqlContext.implicits._
    //("num_type","cid","imsi","imei")
     val monthInput = sqlContext.read.parquet(getInputs(ehlConf,op.hadoopConfiguration):_*)

      .map(f=>(f.getString(0),(f.getLong(1),f.getString(2))))
      .groupByKey()
      .filter(filter=>filter._2.map(f=>f._1).toSet.size>ehlConf.getInt("association.cid.greater.than",2))
        .mapValues(vs=>{
          vs.groupBy(f=>f._2)
            .filter(filter=>{
              filter._2.map(a=>a._1).toSet.size>ehlConf.getInt("association.imsi.cid.greater.than",2)
            }).flatMap(f=>f._2.map(f=>f._2))
            .groupBy(f=>f).mapValues(f=>f.size)
        }).flatMap(f=>{
      f._2.map(a=>{
        val n_type = f._1.split("-")
        (n_type(0),n_type(1),a._1,a._2)
      })
    })
      .toDF()

    /**
      *
      */
//      .map(f=>(f.getString(0),(f.getLong(1),f.getString(2))))
//        .groupByKey()
//      .filter(filter=>filter._2.map(f=>f._1).toSet.size>ehlConf.getInt("association.cid.greater.than",2))
//        .flatMap(f=>{
//          f._2.map(t=>{
//            (f._1+"-"+t._2,t._1)
//          })
//        }).groupByKey()
//      .filter(filter=>filter._2.toSet.size>ehlConf.getInt("association.imsi.cid.greater.than",2))
////      println(monthInput.first())
//      .map(f=>(f._1,f._2.size))
////    println(t1.first())
//      .map(f=>{
//        val n_t_i=f._1.split("-")
//        (n_t_i(0),n_t_i(1),n_t_i(2),f._2)
//      }).toDF()
    /**
      *
      */
    //        .map(f=>(f.getString(0)+"-"+f.getLong(1),(1,f.getString(2))))
//      //同一个车经过三个卡点
//      .reduceByKey((a:(Int,String),b:(Int,String))=>(a._1+b._1,a._2+"\t"+b._2))
//      .filter(f=>f._2._1>2)
//
//        .map(data=>{
//          data._2._2.split("\t",Integer.MAX_VALUE)
//          .map(imsi=>(data._1+"-"+imsi,1))
//        })
//      .flatMap(f=>f)
//        .reduceByKey(_+_).filter(f=>f._2>2)
//      //num-type-cid-imsi
//        .map(f=>{
//          val value = f._1.split("-")
//          ((value(0)+"-"+value(1)+"-"+value(3)),1)
//        })
////      println(monthInput.first())
//      //num,type,imsi,count
//      .reduceByKey(_+_).map(f=>{
//      val n_t_i=f._1.split("-")
//
//      (n_t_i(0),n_t_i(1),n_t_i(2),f._2)
//
//    }).toDF()

    monthInput.write.option("spark.sql.parquet.compression.codec","snappy").parquet(MessageFormat.format(ehlConf.get("result.hdfs.path"),currentDate.plusDays(-1).toString("yyyy-MM-dd")))

    deleteDb("delete from T_ITGS_IMSI_MAPPING",ehlConf)

    saveToDb(monthInput,"INSERT INTO T_ITGS_IMSI_MAPPING(BH,HPHM,HPZL,IMSI,MATCHNUM,UPDATETIME) VALUES ( ?,?,?,?,?,?)",currentDate.toDate,ehlConf)

//      .saveAsTextFile("/app/test/"+args(0))
//          .filter(f=>f._2._1>2)
//        .map()
//          .reduceByKey(_+_)
//          .filter(f=>f._2>2)
  })

  /**
    * num,type,imsi,count
    *
    * @param df
    * @param sql
    * @param date
    * @param conf
    */
  def saveToDb(df:DataFrame,sql:String,date:Date,conf:EhlConfiguration)={
    df.map(f=>{
//      val num_type = f.getString(0).split("-")
//      val num = num_type(0)
//      val t = num_type(1)
      Row(f.getString(0),f.getString(1),f.getString(2),f.getInt(3),new Timestamp(date.getTime))
//      val ps = f.getString(1).split(",")
//      ps.map(data=>{
//        val p_c = data.split("-")
//        Row(num,t,p_c(0),p_c(1).toInt,new Timestamp(date.getTime))
//      })
    })
//      .flatMap(f=>f)
      .foreachPartition(insertDataFunc(_,sql,conf))
  }

  val batchSize=100

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
