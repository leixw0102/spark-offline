package com.ehl.offline.month

import java.io.File

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.impl.PathOfOftenFunction
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, Period, PeriodType}
import org.joda.time.format.DateTimeFormat

/**
  * Created by 雷晓武 on 2016/12/14.
  */
object EhlPathOfOftenBusinessSpark extends PathBusinessTrait{
  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "the path of the often by leixw"

  /**
    * init conf
    *
    * @return
    */
  override def initEhlConfig: EhlConfiguration = {
    val fromSystem = System.getProperty("month_business")
    val conf = if(fromSystem ==null || fromSystem.isEmpty) "month_business.conf" else fromSystem
    new EhlConfiguration().addResource(conf)

  }

  /**
    *  长走路线
    *
    *   from :numb-plateType,time-cid`time-cid,time-cid`time-cid
    *
    *   middle-1:(numb-cid`cid`.etc  num)
    *
    *  result numb,cid-cid.etc,num
    */
  operateSpark(args,ehlConf)(op=>{
    val sql = new SQLContext(op)
    val df =op.textFile(getInputs(ehlConf).mkString(","))

//    val df = sql.createDataset(op.textFile(getInputs(ehlConf).mkString(",")))

  // numb,trip`num,trip`num .etc
   PathOfOftenFunction.executePathOfOftenProcesser(sql,df,args(0))
     // .map(f=>(f.substring(0,f.indexOf(","))))
  })

  /**
    *
    * @param conf
    * @return
    */
  override def getInputs(conf: EhlConfiguration): Array[String] = {
    val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
    val path_prefix=conf.get(basePathKey)
    val from =conf.get(inputFromKey)
    val end = conf.get(inputEndKey)
    val fromDate = DateTime.parse(from,formater)
    val endDate = DateTime.parse(end,formater)
    val period = new Period(fromDate,endDate,PeriodType.days())
    val array = scala.collection.mutable.ArrayBuffer[String]()
    array.+=(path_prefix+File.separator+from)
    for(i<- 1 to period.getDays-1){
      val temp =path_prefix+File.separator+fromDate.plusDays(i).toString("yyyy-MM-dd");
      array+=(temp)
    }
    array+=(path_prefix+File.separator+end)
    array.toArray
  }
}
