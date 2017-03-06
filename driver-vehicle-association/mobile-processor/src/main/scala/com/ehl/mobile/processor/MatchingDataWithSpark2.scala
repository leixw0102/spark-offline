package com.ehl.mobile.processor

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.{MessageFormat, SimpleDateFormat}
import java.util.{Date, Properties}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import com.ehl.mobile.conf.BaseConfigConstant
import com.ehl.mobile.utils.UUIDUtil
import com.ehl.offline.common.{EhlConfiguration, ObjectUtils}
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import oracle.jdbc.OracleDriver
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.types._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

//时间戳,事件类型(27),用户号码,用户归宿局代码,对方号码,对方号码归属地,Imsi,
//Imei(前14位),活动地区,交换机ID,位置区码,基站号,基站经度,基站纬度,上个位置区,
//上个小区,上个基站经度,上个基站纬度
case class MobileBaseData1(timestamp:Long,imsi:String,imei:String,baseStationNumber:Long)
//case class Tracker(number_type:String,cid:Long,time:Array[Long])





/**
  * Created by 雷晓武 on 2017/2/14.
  */
object MatchingDataWithSpark2 extends AbstractSparkEhl with EhlInputConfForHdfsConf with BaseConfigConstant with App{

  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    val formater = DateTimeFormat.forPattern("yyyyMMdd")
    val path_prefix=conf.get(mobilePath)
//    Array(conf.get(mobilePath))
    val from =conf.get(mobileFrom)
    val realFrom = if(from.isEmpty || from == null) DateTime.now().plusDays(-1).toString("yyyyMMdd") else from
    val size = conf.getInt(mobileSize,120)
//
    val fromDate = DateTime.parse(realFrom,formater)
    val array = scala.collection.mutable.ArrayBuffer[String]()
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsConf)
    for(i<- 0 to size){
      val temp =path_prefix+File.separator+fromDate.plusDays(-i).toString("yyyyMMdd");
      println(temp)
      if(exitDirectoryWithHadoop(temp,fs)) array+=(temp)
    }
    fs.close()
    array.toArray
  }


  def getVInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
    val path_prefix=conf.get("v.hdfs.path")
    //    Array(conf.get(mobilePath))
    val from =conf.get("v.hdfs.from")
    val realFrom = if(from.isEmpty || from == null) DateTime.now().plusDays(-1).toString("yyyy-MM-dd") else from
    val size = conf.getInt("v.hdfs.size",120)
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
  override def getSparkAppName: String = "mobile match with tracker "

  override def initEhlConfig: EhlConfiguration = {
    val file = System.getProperty("data","data.conf")
    new EhlConfiguration().addResource(file)
  }

  System.setProperty("java.util.Arrays.useLegacyMergeSort", "true");

  operateSpark(args ,ehlConf)(op=>{


    val shareCards=readFile
    val shareCardsSet=shareCards.values.map(f=>f.toLong).toSet
    println(shareCardsSet.mkString(","))
    val session = new SQLContext(op)
    import session.implicits._
    val currentDate = new Date()

//    val path = getInputs(parser.input,session.hadoopConfiguration);

//    val s=getSplit(ehlConf)
    val lines = op.textFile(getVInputs(ehlConf,op.hadoopConfiguration).mkString(","))
      .map(f => f.split("\t",17)) //args(0)).map(f=>f.split(",",17))
      .filter(f => f.length == 17)
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), "无牌"))
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), "00000000"))
      .filter(f => f(3).split("=")(1) != None)

      .map(f => convertToTracker(f))
        .filter(f=>shareCardsSet.contains(f.cid))
//      .filter(f => shareData.value.equals(f.day)
//      )
//  println(lines.count()+".....")
    val groupValue=lines.groupBy(value=>value.numb+"-"+value.plateType)
//      println(groupValue.first())
        val result1=groupValue.filter(data=>{
          data._2.map(a=>a.cid).toSet.size>=ehlConf.getInt("tracker.greater.than.polt.length",2)
        }).flatMap(f=>f._2).toDF()

//    result1.show(20)
    //passTime:Long,numb:String,plateType:Int,cid:Short
    //lines 为分组过车轨迹
    val cache = result1.cache()
    result1.registerTempTable("v")

//    val trackerDatePath = MessageFormat.format(ehlConf.get(trackerData),DateTime.now().plusDays(-1).toString("yyyy-MM-dd"))
//    println("tracker path "+trackerDatePath)
//    val shareTracker= session.read.parquet(trackerDatePath).select("numb_plateType","cids","times").rdd
//      .filter(f=>{
//        f.getString(1).split("-").length>ehlConf.getInt("tracker.greater.than.polt.length",2)
//      }).filter(f=>{
//      val cs = f.getString(1).split("-")
//      val size=cs.groupBy(f=>f)
//        .mapValues(f=>(f.length.toDouble/cs.length.toDouble).formatted("%.2f").toDouble)
//        .map(f=>f._2).filter(f=>f>ehlConf.getDouble("tracker.filter",0.3))
//      if(size.size>0) true else false
//    })

    val mobileDataRdd=op.wholeTextFiles(getInputs(ehlConf,op.hadoopConfiguration).mkString(","),ehlConf.getInt(mobilePartitionSize,15))
      .map(f=>f._2).map(f=>f.split("\n")).flatMap(f=>f).map(data=>{
      val spliter = data.split(",",25)
      MobileBaseData1(spliter(0).toLong*1000,spliter(6),spliter(7),spliter(11).toLong)
    })
      .filter(f=>shareCards.contains(f.baseStationNumber+"")).map(f=>MobileBaseData1(f.timestamp,f.imsi,f.imei,shareCards.getOrElse(f.baseStationNumber+"","0").toLong))
      .toDF()
    mobileDataRdd.registerTempTable("mobile")
//    println(mobileDataRdd.count()+"\t"+result1.count())
//    //passTime:Long,numb:String,plateType:Int,cid:Short
//    //timestamp:Long,imsi:String,imei:String,baseStationNumber:Long
//    val broadValue=op.broadcast(result1.collect())
    val tempResult = match4(result1.collect(),mobileDataRdd)
    ////numb_type,cid,ts,ts1,imsi,imei
    //(f=>(f._1,f._5,f._6,f._3,f._4))
//    val sqlResult = session.sql("select a.numb,a.plateType,a.cid ,a.passTime,b.timestamp,b.imsi from v as a , mobile as b where a.cid=b.baseStationNumber")
//
////    sqlResult.show(20)
////    println(sqlResult.count()+"\t +sql count")
//    val dd=sqlResult.rdd.filter(f=>{
//
//      val ts = f.getLong(3)
//       val currentTime = new DateTime(ts)
//       val timestamp = f.getLong(4)
//       timestamp>=currentTime.plusSeconds(-120).toDate.getTime && timestamp <= currentTime.plusSeconds(120).toDate.getTime
//       }).map(f=>(f.getString(0)+"-"+f.getInt(1),f.getLong(2),f.getLong(3),f.getLong(4),f.getString(5)))
    //num_type,cid,t1,t2,imsi
    // val abc=session.sql("select a.number_type,a.baseStationNumber,a.ts,b.timestamp,b.imsi,b.imei from tracker a ,mobileData b where a.baseStationNumber = b.baseStationNumber")

    //////numb_type,imsi,imei,ts,ts1
//    (f=>(f._1,f._5,f._2))
    ////numb_type,cid,ts,ts1,imsi,imei
    //(f=>(f._1,f._5,f._6,f._3,f._4))
    val imsi =tempResult.map(f=>((f._1,f._2),f._3)).groupByKey()
        .filter(data=>data._2.map(f=>f).toSet.size >2)
          .mapValues(result=>{
            result.size
//            (result.size,result.mkString(","))
          }).toDF()
//    imsi.show(20,false)
//
    imsi.repartition(20).write.parquet("/app/base/imsi-new1/"+new DateTime(currentDate).plusDays(-1).toString("yyyy-MM-dd"))

//    println("...................................."+mobileDataRdd.count()+"\t"+shareTracker.count())
    //used shareCards and mobileDataRdd data
    //原始数据a
    //每条tracker数据之后b
    //最后union数据b1 union b2 ...etc


//    val vs = readFile.values.toSet
//    val result = shareTracker.map(f => {
//      val ts = f.getString(2).split("`")
//      ts.map(t => (f.getString(0), f.getString(1), t))
//    }).flatMap(f => f)
//      .filter(f=>{
//        val cid = f._2.split("-")
//        val filterSize= cid.filterNot(f=>vs.contains(f)).size
//        if(filterSize==0){true} else{false}
//      })
//      .map(a => {
//        val cid = a._2.split("-")
//        val ts = a._3.split("-")
//        cid.zip(ts).map(b => (a._1, b._1, b._2))
//      }).flatMap(f => f)
//      .toDF("number_type","baseStationNumber","ts")
//
//
//    val dd = match3(result.collect,mobileDataRdd,ehlConf.getInt("mobile.time.between.float",180))
//    dd.cache()

    //修改(a,b),count-->a group b--top
    ////numb_type,imsi,imei,ts,ts1
//    val yesterday=new DateTime(currentDate).plusDays(-1).toString("yyyy-MM-dd")
//    val imsi =dd.map(f=>((f._1,f._2),1)).reduceByKey(_+_).map(f=>(f._1._1,f._1._2,f._2)).groupBy(f=>f._1).mapValues(f=>
//    {
//
//      f.toList.sortWith((a,b)=>a._3>b._3).take(3).map(f=>f._2+"-"+f._3).mkString(",")
//            f.map(f=>f._2+"-"+f._3).mkString(",")
//    })
//      .toDF()
    //    val imsi = dd.map(f=>((f._1,f._2),())).reduce()
//    imsi.cache
//    imsi.write.parquet(MessageFormat.format(ehlConf.get("hdfs.imsi.path"),yesterday))
    //    val imei=dd.map(f=>((f._1,f._3),1)).reduceByKey(_+_).map(f=>(f._1._1,f._1._2,f._2)).groupBy(f=>f._1).mapValues(f=>
    //    {
    //      f.toList.sortWith((a,b)=>a._3>=b._3).take(3).map(f=>f._2+"-"+f._3).mkString(",")
    ////      f.map(f=>f._2+"-"+f._3).mkString(",")
    //    })
    //      .toDF()
    //      imei.cache
    //      imei.write.parquet(MessageFormat.format(ehlConf.get("hdfs.imei.path"),yesterday))

    //TODO save to oracle
//    saveToDb(imsi,"INSERT INTO T_ITGS_IMSI_MAPPING(BH,HPHM,HPZL,IMSI,MATCHNUM,UPDATETIME) VALUES ( ?,?,?,?,?,?)",currentDate,ehlConf)

    //TODO save imei to oracle
//    saveToDb(imei,"INSERT INTO T_ITGS_EMSI_MAPPING(BH,HPHM,HPZL,EMSI,MATCHNUM,UPDATETIME) VALUES ( ?,?,?,?,?,?)",currentDate,ehlConf)

  })

  def match4(shareTracker:Array[Row],mobile:DataFrame,floatingSeconds:Int=180)={
    //share tracker [num_type,cid,ts]
    //mobile [ts1,imsi,imei,cid]
//passTime:Long,numb:String,plateType:Int,cid:Short
    val mapTracker = shareTracker.map(f=>(f.getLong(3),(f.getString(1)+"-"+f.getInt(2),f.getLong(3))))
      .groupBy(f=>f._1)
    mobile.rdd.map(f => {
      val cid = f.getLong(3)
      mapTracker.get(cid).map(array=>{
        array.map(a=>{
          (a._2._1,cid,a._2._2,f.getLong(0),f.getString(1),f.getString(2))
        })
      })
      //numb_type,cid,ts,ts1,imsi,imei

    }).flatMap(f=>f).flatMap(f=>f)
      .filter(f=>{
        val ts = f._3
        val currentTime = new DateTime(ts)
        val timestamp = f._4.toLong
        timestamp>=currentTime.plusSeconds(-floatingSeconds).toDate.getTime && timestamp <= currentTime.plusSeconds(floatingSeconds).toDate.getTime
      }).map(f=>(f._1,f._5,f._2))
  }


  // DRIVEWAY 车道编号  drivedir 行驶方向
  case class Tracker(passTime:Long,numb:String,plateType:Int,cid:Long){
    override def toString={
      passTime+"\t"+numb+"\t"+cid+"\t"
    }
  }
  def convertToTracker(f:Array[String]):Tracker={
    val timeString = (f(1).split("="))(1)
    val passtime = toDate(timeString,f)
    val t=Tracker(passtime._1,
      f(3).split("=")(1),
      toInt(f(4).split("=")(1),f),
      toLong(f(15).split("=")(1),f))

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
  def getFilterQuery(ts:Long):String={
    val currentTime = new DateTime(ts)
    " timestamp >="+currentTime.plusSeconds(-10).toDate.getTime +" and timestamp<="+currentTime.plusSeconds(10).toDate.getTime
  }


  def saveToDb(df:DataFrame,sql:String,date:Date,conf:EhlConfiguration)={
    df.map(f=>{
      val num_type = f.getString(0).split("-")
      val num = num_type(0)
      val t = num_type(1)
      val ps = f.getString(1).split(",")
      ps.map(data=>{
        val p_c = data.split("-")
        Row(num,t,p_c(0),p_c(1).toInt,new Timestamp(date.getTime))
      })
    }).flatMap(f=>f)
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

  /**
    * 读取卡点与手机信令的对应关系
    *
    * @return
    */
  def readFile:Map[String,String]={
    val dictionaryFile = System.getProperty("base_dictionary","base_dictionary.dat")
    val conf = new EhlConfiguration().addResource(dictionaryFile)
    conf.toMap()
  }

}
//    result.show(30,false)
///////////////////////////////////////////
/**  result.registerTempTable("tracker")
  * val abc=session.sql("select a.number_type,a.baseStationNumber,a.ts,b.timestamp,b.imsi,b.imei from tracker a ,mobileData b where a.baseStationNumber = b.baseStationNumber")
  * val dd=abc.rdd.filter(f=>{
  * val ts = f.getString(2).toLong
  * val currentTime = new DateTime(ts)
  * val timestamp = f.getLong(3)
  * timestamp>=currentTime.plusSeconds(-10).toDate.getTime && timestamp <= currentTime.plusSeconds(10).toDate.getTime
  * }).map(f=>(f.getString(0),f.getString(4),f.getString(5)))
  **/
//////////////////////////////////////////////////////////////////


//思路 同一轨迹找交集
//    shareTracker.foreachPartition()


//思路1
//    shareTracker.map(data=>{
//      println("tracker data ="+data.mkString(","))
//      val number_type=data.getString(0)
//      val cids = data.getString(1).split("-")
//      val times = data.getString(2).split("`")
//
//      val filterRdd = mobileDataRdd.filter("baseStationNumber in ("+cids.mkString(",")+")")
//      println("filter condition ='baseStationNumber in ("+cids.mkString(",")+")" + filterRdd.count())
//      filterRdd.cache()
//
//      val firstMath = times.map(f=>f.split("-")).flatMap(f=>f).sorted.sliding(2,2).map(f=>{
//        if(f.length==2){
//          val firstTime = f(0)
//          val lastTime=f(1)
//          (filterRdd.select("timestamp","imsi","imei").where( getFilterQuery(firstTime.toLong)).select("imsi","imei"))
//            .unionAll( filterRdd.select("timestamp","imsi","imei").where( getFilterQuery(lastTime.toLong)).select("imsi","imei"))
//        }else{
//          filterRdd.select("timestamp","imsi","imei").where(getFilterQuery( f(0).toLong)).select("imsi","imei")
//        }
//      }).reduceLeft((a,b)=>a.unionAll(b))
//      val imsiResult = firstMath.groupBy("imsi").count().sort($"count".desc).take(3)
//      val imeiResult = filterRdd.groupBy("imei").count().sort($"count".desc).take(3)
//      println("imei result "+imeiResult.mkString(","))
//      println("imsi result "+imsiResult.mkString(","))
//    })
//思路2
//    shareTracker.map(f=>{
//      val cids = f.getString(1)
//      val times = f.getString(2)
//
//      val cs = cids.split("-")
//      val ts = times.split("`")
//      cs.map(t=>Tracker(f.getString(0),t.toLong,))
//    })

///////////////////////////////////////////////////////////////////////////