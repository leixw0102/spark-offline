package com.ehl.offline.base
import java.util.Date

import com.ehl.offline.business.{BaseConfigConstant, DataBaseFunction, VehicleTripFunction}
import com.ehl.offline.common.{EhlConfiguration, ObjectUtils}
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlFilterInputFromHdfsForShell
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SQLContext

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * 卡点对
  * Created by 雷晓武 on 2016/12/6.
  */
object EhlBaseDataProcesser extends AbstractSparkEhl with EhlFilterInputFromHdfsForShell with BaseConfigConstant with App{
    val defaultSplit="\t"

  def getSplit(conf:EhlConfiguration):String={
    val temp = conf.get(split)
    if(temp == null || temp.isEmpty)
    { println(".....")
      defaultSplit} else {temp}
  }
  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "ehl-base-data-processer by lxw"+new Date().toString

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
    val shareData = session.broadcast(args(1))
//    val sql = new SQLContext(session)
//    implicit sql.implicits._
    val path = getInputs(args(0),session.hadoopConfiguration);

    val s=getSplit(ehlConf)
    val lines = session.textFile(path)
      .map(f => f.split(s,17)) //args(0)).map(f=>f.split(",",17))
      .filter(f => f.length == 17)
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), ehlConf.get(noCardKey)))
      .filter(f => ObjectUtils.noEqual(f(3).split("=")(1), ehlConf.get(errorCardKey)))
      .filter(f => f(3).split("=")(1) != None)
      .map(f => DataBaseFunction.convertToTracker(f))
      .filter(f => shareData.value.equals(f.day)
      ).groupBy(value=>value.numb+"-"+value.plateType)
    //lines 为分组过车轨迹
    val cache = lines.cache()
    val filterResult = DataBaseFunction.generatedPairAndRemoveDirtyData(cache);

    //过车卡点对生成保存  格式 short-short,int,long
    DataBaseFunction.pairAndSave(filterResult,args(2));

    //根据规则生成过车轨

    //后期再重构， //保存格式  numb,time-cid`time-cid,time-cid`time-cid
    VehicleTripFunction.generatedTripTracker(cache,args(3))

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
