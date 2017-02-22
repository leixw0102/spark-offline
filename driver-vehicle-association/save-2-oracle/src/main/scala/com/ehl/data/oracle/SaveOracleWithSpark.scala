package com.ehl.data.oracle

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration

/**
  * Created by 雷晓武 on 2017/2/18.
  */
object SaveOracleWithSpark extends AbstractSparkEhl with EhlInputConfForHdfsConf  with App{
  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    Array("")
  }

  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "save 2 oracle"

  override def initEhlConfig: EhlConfiguration = {
    new EhlConfiguration().addResource("oracle.conf")
  }

  operateSpark(args ,ehlConf)(sc=>{

    val test =sc.textFile(args(0))
    println("------+")
    println(test.first())
    println(test.collect())
    println("----+")
  })

}
