package com.ehl.offline

import java.text.MessageFormat

import com.ehl.offline.common.EhlConfiguration

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val a="1499058797,27,,,,,460025540828626,00000000000000,,,0530,21000,115.462070,35.288680,,,,";

    println(a.split(",",25).length)

    val str = "/app/base/mobile/{0}"
    println(MessageFormat.format(str,"*"+"2017-07-07".replaceAll("-","")))

//    val a=List((1,2),(1,3),(1,3))
//    a.groupBy(f=>f._1).mapValues(f=>{
//      f.groupBy(f=>f._2).mapValues(f=>f.size).toList.sortWith((a,b)=>a._2>=b._2)
//    }).foreach(println)
//    a.groupBy(f=>f).aggregate()
//    a.groupBy(f=>f).mapValues(f=>f.size).toList.sortWith((a,b)=>a._2>=b._2).take(1).foreach(println)
//    new EhlConfiguration().addResource("base_dictionary.dat").foreach()
//    println( "Hello World!" )
//    println("concat arguments = " + foo(args))
//  val a= List(1,2,3,4,5)
//    a.sliding(2,2).map(f=>f.mkString(",")).foreach(println)
  }

}
