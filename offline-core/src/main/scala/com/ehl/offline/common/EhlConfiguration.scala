package com.ehl.offline.common
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.util.concurrent.ConcurrentHashMap

/**
  * Created by 雷晓武 on 2016/12/12.
  */
class EhlConfiguration extends Cloneable with Serializable{
  /**
    *
    */
  private val settings = new ConcurrentHashMap[String, String]()

  def foreach()={
    for((k,v)<- settings){
      println(k+"\t"+v)
    }
  }

  def toMap():Map[String,String]=settings.toMap

  def getStartWithNameMap(name:String):Map[String,String]={
    val kvs = for{(k,v)<- settings
                  if k.startsWith(name)}yield {(k,v)}
    kvs.toMap
  }

  def addResource(conf:String):EhlConfiguration={
    val pro = Option(PropertiesLoaderUtils.loadAllProperties(conf))
//    pro.get.keys().asScala.foreach(println)
    pro match {
      case Some(p)=>
//        p.stringPropertyNames().asScala.map(f=>{
//          settings.put(f,p.getProperty(f))
//        })
        for(k <- p.keySet()){
          settings.put(k.toString,p.getProperty(k.toString))
        }
      case None=>println("properties null ")
    }

//    val temp = pro.asInstanceOf[Map[String,String]]
//    for((k,v) <- temp){
//      settings.put(k,v)
//    }
//    pro.map(p=>p.keys()).foreach(f=>
//      settings.put(f.toString,pro.get.getProperty(f.toString))
//    )
    this;
  }
  def getOption(key: String): Option[String] = {
    Option(settings.get(key)) //get or else
  }

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def set(key: String, value: String): EhlConfiguration = {
    if(key ==null || key.isEmpty){
      throw new NullPointerException("null key")
    }

    if( value == null || value.isEmpty){
      throw new NullPointerException("null value for " + key)
    }

    settings.put(key,value)
    this;
  }


  /**
    *
    * @param key
    * @return
    */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getInt(key:String, defaultValue: Int):Int={
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getLong(key:String, defaultValue: Long):Long={
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key:String, defaultValue: Double):Double={
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  def getShort(key:String, defaultValue: Short):Short={
    getOption(key).map(_.toShort).getOrElse(defaultValue)
  }

}
