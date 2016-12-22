package com.ehl.offline
import scala.collection.JavaConverters._
/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    System.getProperties.stringPropertyNames().asScala
        .foreach(f=>println(f+"\t"+System.getProperty(f)))
  }

}
