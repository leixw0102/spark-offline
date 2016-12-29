package com.ehl.offline.core

import java.nio.file.{Path, Paths}

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by 雷晓武 on 2016/12/6.
  */
abstract class AbstractSparkEhl extends SparkOp with App{

  /**
    * 設置hadoop配置
    */
  override def setHadoopConfig(sc: SparkContext): Unit = {
//    val core_site=System.getProperty("core-conf","core-site.xml")
//    val hdfs_site=System.getProperty("hdfs-conf","hdfs-site.xml")
//    val c_file = Paths.get(core_site)
//    val h_file = Paths.get(core_site)
//    println(core_site+"\t"+hdfs_site+"---------------")
//
//    sc.hadoopConfiguration.addResource(core_site)
//    sc.hadoopConfiguration.addResource(new org.apache.hadoop.fs.Path(h_file.normalize().toAbsolutePath.toString))
//    println(h_file.normalize().toAbsolutePath.toString)
//    println(sc.hadoopConfiguration.get("dfs.nameservices")+"\t"+sc.hadoopConfiguration.get("fs.defaultFS")+"\t"+sc.hadoopConfiguration.get("ha.zookeeper.quorum") +"\t"+sc.hadoopConfiguration.get("dfs.namenode.rpc-address.cluster1.nn1"))
  }

   def operateSpark(args:Array[String],ehlConf:EhlConfiguration)(op:SparkContext=>Unit){

    //first
    val conf=new SparkConf().setAppName(getSparkAppName)//.setMaster("local[*]")
    //    val session= SparkSession.builder().config(conf).getOrCreate()

    val session = new SparkContext(conf)
    try{
      //保存hadoop

      //     val sc = session.sparkContext
      setHadoopConfig(session)

      op(session)
    }catch{
      case ex:Exception=>ex.printStackTrace()
    } finally{
      //end
      session.stop()
    }
  }

}
