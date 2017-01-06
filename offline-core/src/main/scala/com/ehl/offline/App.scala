package com.ehl.offline
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
   println( TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES).toInt)
    val c:Consumer[Animal,Bird] = new Consumer[Animal,Bird]()
    val c2:Consumer[Bird,Animal] = c
    c2.m1(new Animal)
    c2.m2(new Bird)
  }

  class Animal {}
  class Bird extends Animal {}
  class Consumer[-S,+T]()(implicit m1:ClassTag[T]) {
    def m1[U >: T](u: U): T = {
      println( m1.runtimeClass.newInstance.asInstanceOf[T])

      m1.runtimeClass.newInstance.asInstanceOf[T]} //协变，下界
    def m2[U <: S](s: S): U = {m1.runtimeClass.newInstance.asInstanceOf[U]} //逆变，上界
  }

  class A {
    def abc[x : AutoCloseable](c:x)(handler:x=>Unit): Unit ={
      handler(c)
    }
  }


}
