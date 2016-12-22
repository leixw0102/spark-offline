package com.ehl.offline.core

import com.ehl.offline.common.EhlConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 雷晓武 on 2016/12/6.
  */
abstract class AbstractSparkEhl extends SparkOp with App{

  /**
    * 設置hadoop配置
    */
  override def setHadoopConfig(sc: SparkContext): Unit = {
    sc.hadoopConfiguration.addResource("core-site.xml")
    sc.hadoopConfiguration.addResource("hdfs-site.xml")
  }

  abstract def operateSpark(args:Array[String],ehlConf:EhlConfiguration)(op:SparkContext=>Unit){

    //first
    val conf=new SparkConf().setAppName(getSparkAppName)//.setMaster("local[*]")
    //    val session= SparkSession.builder().config(conf).getOrCreate()

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
