package com.ehl.offline.time.strategy

import java.io.File

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SQLContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 雷晓武 on 2017/1/16.
  */
object HistoryAVGTime extends AbstractSparkEhl with EhlInputConfForHdfsConf with CardTimeConstant{
    override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
      val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
      val path_prefix=conf.get(basePathKey)
      val from =conf.get(inputFromKey)
      val realFrom = if(from.isEmpty || from == null) DateTime.now().toString("yyyy-MM-dd") else from
      val size = conf.getInt(inputSetpSize,240)

      val fromDate = DateTime.parse(realFrom,formater)
      val array = scala.collection.mutable.ArrayBuffer[String]()
      val fs = FileSystem.get(hdfsConf)
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
  override def getSparkAppName: String = "avg time of history "

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource("base.conf")
  }


  operateSpark(args,ehlConf){op=>{
    val sql = new SQLContext(op)
    val inputs = getInputs(ehlConf,op.hadoopConfiguration)
    if(inputs.length==0) throw new Exception("输入为空")
    val df =op.textFile(inputs.mkString(","))
    //过车卡点对生成保存
    // 格式 short-short,startTime,endTime,timestamp`startTime,endTime,timestamp...etc
    val rdd = df.map(f=>{
      val cards = f.substring(0,f.indexOf(","))
      val times = f.drop(f.indexOf(",")+1)
      val array = scala.collection.mutable.ArrayBuffer[CardBase]()
      for(line <- times.split("`",Integer.MAX_VALUE)){
        val ts = line.split(",")
        array+=CardBase(cards,ts(0).toLong,ts(1).toLong,ts(2).toLong)
      }
      array
    }).flatMap(f=>f)
    //格式 cards`start`end \t avg
      HistoryCala.timeProcesserAndSave(rdd,
        ehlConf.getLong(maxTimeKey,3600)*1000,
        ehlConf.getDouble(downFloor,0.03),
        ehlConf.getDouble(upFloor,0.10),
        args(0))

  }}

}
case class CardBase(cards:String,start:Long,end:Long,time:Long)



