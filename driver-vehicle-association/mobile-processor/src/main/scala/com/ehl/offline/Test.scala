package com.ehl.offline

import com.ehl.mobile.conf.BaseConfigConstant
import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration

/**
  * Created by 雷晓武 on 2017/2/17.
  */
object Test extends AbstractSparkEhl with EhlInputConfForHdfsConf with App{
  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    Array("")
  }

  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "test"

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource("data.conf")
  }

  operateSpark(args ,ehlConf )(sc=>{
//    sc.textFile("/test")
  })
}
