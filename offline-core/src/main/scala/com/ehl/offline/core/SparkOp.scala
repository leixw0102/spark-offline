

/**
 * @author ehl
 */
package com.ehl.offline.core

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
 *
 * @author ehl
 */
trait SparkOp{


  /**
    * 获取spark app name
    *
    * @return
    */
  def getSparkAppName:String

  /**
    * 設置hadoop配置
    */
  def setHadoopConfig(sc:SparkContext):Unit

  def initEhlConfig:EhlConfiguration


  val ehlConf=initEhlConfig
  /**
    * sparkContext
    *(op:(SparkContext)=>Unit)
    */
  def operateSpark(args:Array[String],ehlConf:EhlConfiguration)(op:SparkContext=>Unit)

}