

/**
 * @author ehl
 */
package com.ehl.offline.core.es

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
 *
 * @author ehl
 */
trait SparkWithEsOp{



  def setESConfig(sc:SparkConf,ehlConfiguration: EhlConfiguration):Unit


  /**
    * sparkContext
    *(op:(SparkContext)=>Unit)
    */
  def operateSpark(args:Array[String],ehlConf:EhlConfiguration)(op:SparkContext=>Unit)(implicit useEs : Boolean=false){

    //first
    val conf=new SparkConf().setAppName(getSparkAppName)//.setMaster("local[*]")
//    val session= SparkSession.builder().config(conf).getOrCreate()
    setESConfig(conf,ehlConf)
    val session = new SparkContext(conf)
    try{
      //保存hadoop

//     val sc = session.sparkContext
     setHadoopConfig(session);
     op(session)
    }catch{
      case ex:Exception=>ex.printStackTrace()
    } finally{
      //end
      session.stop()
    }
  }

}