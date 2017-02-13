package com.ehl.offline.base

import java.text.MessageFormat
import java.util.Date

import com.ehl.offline.business.{BaseConfigConstant, DataBaseFunction, VehicleTripFunction}
import com.ehl.offline.common.{EhlConfiguration, ObjectUtils}
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlFilterInputFromHdfsForShell
import com.ehl.offline.time.strategy.TimeLine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.DateTime

/**
  * Created by 雷晓武 on 2017/1/22.
  */
object EhlBaseTrackerSpark extends AbstractSparkEhl with EhlFilterInputFromHdfsForShell with BaseConfigConstant with App{
  val defaultSplit="\t"

  def getSplit(conf:EhlConfiguration):String={
    val temp = conf.get(split)
    if(temp == null || temp.isEmpty)
    {
      defaultSplit} else {temp}
  }
  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "ehl-base-data-processer by lxw"+new Date().toString
  private  val parser = new TrackerParserImpl()

  /**
    * VERSION=1.0,
    * PASSTIME=2016-11-30 16:58:41 000,
    * CARSTATE=1,
    * CARPLATE=鲁RNP768,
    * PLATETYPE=2,
    * SPEED=0,
    * PLATECOLOR=2,
    * LOCATIONID=-1,
    * DEVICEID=-1,
    * DRIVEWAY=7,
    * DRIVEDIR=2,
    * CAPTUREDIR=1,
    * CARCOLOR=1,
    * CARBRAND=99,
    * CARBRANDZW=其它,
    * TGSID=1012,
    * PLATECOORD=2530,1215,2663,1247,
    * CABCOORD=0,0,0,0,
    * IMGID1=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbXR6AAtqFZ4oge0586.jpg,
    * IMGID2=,
    * IMGID3=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbefZAAAIquqTDOk722.jpg,
    *
    *
    *  VERSION=1.0,PASSTIME=2016-11-30 16:58:41 000,CARSTATE=1,CARPLATE=鲁RNP768,PLATETYPE=2,SPEED=0,PLATECOLOR=2,LOCATIONID=-1,DEVICEID=-1,DRIVEWAY=7,DRIVEDIR=2,CAPTUREDIR=1,CARCOLOR=1,CARBRAND=99,CARBRANDZW=其它,TGSID=1012,PLATECOORD=2530,1215,2663,1247,CABCOORD=0,0,0,0,IMGID1=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbXR6AAtqFZ4oge0586.jpg,IMGID2=,IMGID3=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbefZAAAIquqTDOk722.jpg,
    */
  operateSpark(args,ehlConf)((session) => {
    //    val pro = System.getProperties.asScala.foreach(f=>println(f._1+"\t"+f._2))
    parser.parser(args.toList)

    val shareData = session.broadcast(parser.shareData)
    //    val sql = new SQLContext(session)
    //    implicit sql.implicits._
    val path = getInputs(parser.input, session.hadoopConfiguration);

    val s = getSplit(ehlConf)
    val lines = session.textFile(path)
      .map(f => f.split(s, 17)) //args(0)).map(f=>f.split(",",17))
      .filter(f => f.length == 17)
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), ehlConf.get(noCardKey)))
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), ehlConf.get(errorCardKey)))
      .filter(f => f(3).split("=")(1) != None)
      .map(f => DataBaseFunction.convertToTracker(f))
      .filter(f => shareData.value.equals(f.day)
      ).groupBy(value => value.numb + "-" + value.plateType)
    //lines 为分组过车轨迹
    val cache = lines.cache()
    val before = ehlConf.getInt("time.strategy.avg.before.day",1)
    val avgPath = ehlConf.get("time.strategy.avg.input","/app/base/history_time/{0}")
    val day =new DateTime(new Date()).plusDays(-before).toString("yyyy-MM-dd")
    val historyPath =MessageFormat.format(avgPath,day)
    val cacheRdd = session.textFile(historyPath).map(f=>{
      val lines = f.split("\t")
      (lines(0),lines(1).toLong)
    })
    val broadcast = session.broadcast(cacheRdd.collectAsMap())
    val timeLine = TimeLine.getClass(ehlConf,broadcast)

    VehicleTripFunction.generatedTripTracker(cache,parser.vehicleLineOutput,timeLine)
  })
  /**
    * bug
    *
    * @return
    */
  override def initEhlConfig: EhlConfiguration = {
    //init conf
    //may be from if [ -z "$PUBLISH_AKKA_OPTS" ]; then
    //PUBLISH_AKKA_OPTS="-Dpublish-akka=$base_dir/conf/akka.conf"
    //fi

    val fromSystem = System.getProperty("base-conf","base.conf")

    val conf = new EhlConfiguration().addResource(fromSystem)
    conf.foreach()
    conf;
  }
  override def getInputs(path: String, conf: Configuration): String = {
    val fs = FileSystem.get(conf)
    val listStatus = fs.listStatus(new Path(path))
    val result = for{status <- listStatus ;if (status.getPath.toString.endsWith(".writed"))} yield {status.getPath.toString.replace(".writed","")}
    fs.close()
    if(result.isEmpty || result.length==0) path else result.mkString(",")
  }
}

class TrackerParserImpl extends Serializable{

  var input = ""
  var cardsOutput = ""
  var shareData=new DateTime().plusDays(-1).toString("yyyy-MM-dd")

  var vehicleLineOutput=""

  def printUsageAndExit(i: Int): Unit = {
    System.err.println(
      "Usage: process data [options]\n" +
        "\n" +
        "Options:\n" +
        "  -i input     input path of hdfs \n" +
        "  -share day   filter data of 'day',default format of  'yyyy-MM-dd' for yesterday  \n" +
        "  -cards output  the path of two cards \n" +
        "  -vl vehicle line      the path of vehicle line \n" +
        "  --help println help.\n" +
        "                         Default is conf/spark-defaults.conf.")
    System.exit(i)
  }

  def parser(args: List[String]): Unit = args match {
    case ("-i") :: value :: tail=>
      input = value
      parser(tail)
    case ("-share")::value ::tail=>
      shareData = value
      parser(tail)
//    case ("-cards") :: value :: tail=>
//      cardsOutput = value
//      parser(tail)
    case ("-vl") :: value :: tail =>
      vehicleLineOutput = value;
      parser(tail)
    case ("--help") :: tail =>
      printUsageAndExit(0)

    case Nil => {}

    case _ =>
      printUsageAndExit(1)
  }

  override def toString()={input+"\t"+shareData+"\t"+cardsOutput+"\t"+vehicleLineOutput}
}
