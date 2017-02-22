package com.ehl.offline

import java.nio.file.{Path, Paths}

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val p = Paths.get("c:\\a")
    println(p.resolve("abc"))
  }

}
