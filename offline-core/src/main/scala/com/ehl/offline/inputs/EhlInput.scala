package com.ehl.offline.inputs

import com.ehl.offline.common.EhlConfiguration
import org.apache.hadoop.conf.Configuration

/**
  * Created by 雷晓武 on 2016/12/14.
  *
  * from shell command
  */
trait EhlInputForShell {
  def getInputs(path:String):String
}

/**
  * 丛培智文件读取
  */
trait EhlInputForConf{
    def getInputs(conf:EhlConfiguration):Array[String]
}

/**
  * 从配置文件读取,依赖hdsf文件判断  //TODO
  * TODO 跟EhlInputForConf 合并
  */
trait EhlInputConfForHdfsConf extends EhlInputForConf{
    def getInputs(conf:EhlConfiguration,hdfsConf:Configuration):Array[String]
}

/**
  *从shell读取  //TODO 跟EhlInputForShell合并
  */
trait EhlFilterInputFromHdfsForShell extends EhlInputForShell{
  def getInputs(path:String,conf:Configuration):String
  def getInputs(path:String):String=getInputs(path,new Configuration())
}

