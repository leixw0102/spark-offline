package com.ehl.offline

import java.io.File

import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.conf.PathOfOftenConfConstant
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Period, PeriodType}

/**
 * @author ${user.name}
 */
object App extends PathOfOftenConfConstant{
  val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  def getInputs(conf: EhlConfiguration): Array[String] = {
    val formater = DateTimeFormat.forPattern("yyyy-MM-dd")
    val path_prefix=conf.get(basePathKey)
    val from =conf.get(inputFromKey)
    val end = conf.get(inputEndKey)
//    println(from+"\t"+end+"\t"+path_prefix+"----------------------")
    val fromDate = DateTime.parse(from,formater)
    val endDate = DateTime.parse(end,formater)
    val period = new Period(fromDate,endDate,PeriodType.days())
      println(fromDate.toString("yyyy-MM-dd")+"\t"+endDate.toString("yyyy-MM-dd")+"\t"+period)
    val array = scala.collection.mutable.ArrayBuffer[String]()
    array.+=(path_prefix+File.separator+from)
    for(i<- 1 to period.getDays-1){
      val temp =path_prefix+File.separator+fromDate.plusDays(i).toString("yyyy-MM-dd");
//      println(temp)
      array+=(temp)
    }
    array+=(path_prefix+File.separator+end)
    array.toArray
  }
  def main(args : Array[String]) {

    val d = new DateTime()
    println(d.getHourOfDay)

//    val array = getInputs(new EhlConfiguration().addResource("month_business.conf"))
//    array.foreach(println)
//
//    val test="鲁RD5666,1480735193000-1010`1480736050000-1011`1480736393000-1010`1480737209000-1003,1480743841000-1003`1480744414000-1010`1480744468000-1011"
//
//      val array = test.split(",")
//       for(temp <- array.drop(1) ){
//         val cids = for(temp<-temp.split("`");cid = temp.split("-")(1)) yield{ cid}
//         println(cids.mkString("-"))
//       }

  }

}
