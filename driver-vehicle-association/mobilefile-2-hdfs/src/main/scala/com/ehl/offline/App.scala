package com.ehl.offline

import java.nio.file.{FileVisitResult, Path, Paths, StandardCopyOption}

import scala.util.matching.Regex

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val s = "EVT.20170426030840_1493147320.0001.dat";
    val a = "Encrypt_EVT.20170419094248_1492566168.0001.dat"
//    val pattern = new Regex(""".*(\d{8})\d{6}_\d{10}.\d{4}.dat""", "g")
val pattern = new Regex("""\d{8}""")
    pattern.findFirstMatchIn(a) match {
      case Some(date)=>{

       println(date.group(1))
      }
      case None=>println("the false of the matching result ;the pattern ="+pattern+" and the source ")
    }
  }
}
