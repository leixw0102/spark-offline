package com.ehl.offline.es

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.{SparkConf}

/**
  * Created by 雷晓武 on 2016/12/21.
  */

trait ESConfConstant{
  val autoCreate="es.index.auto.create";
  val nodes="es.nodes"
  val port="es.port"
  val exportPathIndexType="es.save.path.often"
}
