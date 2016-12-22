package com.ehl.offline.core

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.es.SparkWithEsOp
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 雷晓武 on 2016/12/6.
  */
abstract class AbstractSparkWithEhl extends AbstractSparkEhl with SparkWithEsOp with App{

  override def setESConfig(sc:SparkConf,ehlConfiguration: EhlConfiguration):Unit={
    val esConf = ehlConfiguration.getStartWithNameMap("es")
    for((k,v)<-esConf){
      sc.set(k,v)
    }
  }

  override def operateSpark(args:Array[String], ehlConf:EhlConfiguration)(op:SparkContext=>Unit){

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
