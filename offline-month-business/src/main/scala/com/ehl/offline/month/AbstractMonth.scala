package com.ehl.offline.month

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by 雷晓武 on 2017/1/11.
  */
abstract class AbstractMonth extends AbstractSparkEhl{
  override def initEhlConfig: EhlConfiguration = {
    val fromSystem = System.getProperty("month_business","month_business.conf")
    val conf= new EhlConfiguration().addResource(fromSystem)
    conf.foreach()
    conf
  }


}
