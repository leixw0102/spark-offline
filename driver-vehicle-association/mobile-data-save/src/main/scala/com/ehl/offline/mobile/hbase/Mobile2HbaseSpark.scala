package com.ehl.offline.mobile.hbase

import java.text.MessageFormat

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.core.AbstractSparkEhl
import com.ehl.offline.inputs.EhlInputConfForHdfsConf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.joda.time.DateTime
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat

case class MobileBaseData(timestamp:Long,imsi:String,imei:String,baseStationNumber:Long)
case class MobileCarBaseData(timestamp:Long,imsi:String,imei:String,baseStationNumber:Long,cid:Long)
/**
  * Created by 雷晓武 on 2017/2/22.
  */
object Mobile2HbaseSpark extends AbstractSparkEhl with EhlInputConfForHdfsConf with App{
  override def getInputs(conf: EhlConfiguration, hdfsConf: Configuration): Array[String] = {
    val path = conf.get("mobile.hdfs.path")
    val yesterday = DateTime.now().plusDays(-1).toString("yyyyMMdd")
    Array(MessageFormat.format(path,yesterday))
  }

  /**
    * 获取spark app name
    *
    * @return
    */
  override def getSparkAppName: String = "mobile data of hdfs to hbase"

  override def initEhlConfig: EhlConfiguration = {
    val file = "mobile.conf";//System.getProperty("","mobile.conf")
    new EhlConfiguration().addResource(file)
  }


  operateSpark(args ,ehlConf )(sc=>{

    val cidMap=readFile
    val values = sc.wholeTextFiles(getInputs(ehlConf).mkString(","),ehlConf.getInt("spark.partation",20))
      //values
      .map(f=>f._2).map(f=>f.split("\n")).flatMap(f=>f)

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",ehlConf.get("hbase.zookeeper.quorum"))
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", ehlConf.get("hbase.zookeeper.port"))
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, ehlConf.get("hbase.table"))
    values.map(data=>{
      val spliter = data.split(",",25)
      MobileBaseData(spliter(0).toLong*1000,spliter(6),spliter(7),spliter(11).toLong)
    })
      .filter(f=>cidMap.contains(f.baseStationNumber+""))
      .map(f=>MobileCarBaseData(f.timestamp,f.imsi,f.imei,f.baseStationNumber,cidMap.getOrElse(f.baseStationNumber+"","0").toLong))
      .map(baseData=>{
        val put = new Put(Bytes.toBytes(baseData.cid+"-"+baseData.timestamp))
        put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("ts"),Bytes.toBytes(baseData.timestamp))
        put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("imsi"),Bytes.toBytes(baseData.timestamp))
        put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("imei"),Bytes.toBytes(baseData.timestamp))
        put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("mobile_cid"),Bytes.toBytes(baseData.timestamp))
        put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("cid"),Bytes.toBytes(baseData.timestamp))
        (new ImmutableBytesWritable(put.getRow),put)
      }).saveAsHadoopDataset(jobConf)
//      .toDF()
  })

  def readFile:Map[String,String]={
    val dictionaryFile = System.getProperty("base_dictionary","base_dictionary.dat")
    val conf = new EhlConfiguration().addResource(dictionaryFile)
    conf.toMap()
  }
}


