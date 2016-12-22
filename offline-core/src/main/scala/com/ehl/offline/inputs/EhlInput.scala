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


trait EhlInputForConf{
    def getInputs(conf:EhlConfiguration):Array[String]
}

/**
  *
  */
trait EhlFilterInputFromHdfsForShell extends EhlInputForShell{
  def getInputs(path:String,conf:Configuration):String
  def getInputs(path:String):String=getInputs(path,new Configuration())
}

/**
  *
  */

