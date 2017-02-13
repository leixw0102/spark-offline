//package com.ehl.offline.month
//
//import java.io.File
//import java.util.Date
//
//import com.ehl.offline.common.EhlConfiguration
//import com.ehl.offline.inputs.EhlInputConfForHdfsConf
//import com.ehl.offline.time.strategy.CardTimeConstant
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.FileSystem
//import org.joda.time.DateTime
//import org.joda.time.format.DateTimeFormat
//
///**
//  * Created by 雷晓武 on 2017/1/11.
//  */
//object EhlCardTimeSpark extends  AbstractMonth with App with EhlInputConfForHdfsConf with CardTimeConstant{
//  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
//    val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
//    val path_prefix=conf.get(basePathKey)
//    val from =conf.get(inputFromKey)
//    val realFrom = if(from.isEmpty || from == null) DateTime.now().toString("yyyy-MM-dd") else from
//    //    val end = conf.get(inputEndKey)
//    val size = conf.getInt(inputSetpSize,240)
//
//    val fromDate = DateTime.parse(realFrom,formater)
//    //    val endDate = DateTime.parse(end,formater)
//    //    val period = new Period(fromDate,endDate,PeriodType.days())
//    val array = scala.collection.mutable.ArrayBuffer[String]()
//    //    array.+=(path_prefix+File.separator+from)
//    val fs = FileSystem.get(hdfsConf)
//    for(i<- 0 to size){
//      val temp =path_prefix+File.separator+fromDate.plusDays(-i).toString("yyyy-MM-dd");
//      println(temp)
//      if(exitDirectoryWithHadoop(temp,fs)) array+=(temp)
//    }
//    fs.close()
//    //    array+=(path_prefix+File.separator+end)
//    array.toArray
//  }
//
//  /**
//    * 获取spark app name
//    *
//    * @return
//    */
//  override def getSparkAppName: String = "the max time of cross card "+new Date().toLocaleString
//
//  /**
//    * 卡点时间计算 半小时粒度
//    */
//  operateSpark(args,ehlConf )(op=>{
//    val inputs = getInputs(ehlConf,op.hadoopConfiguration)
//    if(inputs.length==0) throw new Exception("输入为空")
//    val df =op.textFile(inputs.mkString(","))
//
//    val maxTime = ehlConf.getLong(maxTimeKey,3600L)
//
//  })
//
//}
