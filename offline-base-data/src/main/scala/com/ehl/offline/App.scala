package com.ehl.offline

import com.ehl.offline.common.EhlConfiguration


/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)

  def main(args : Array[String]) {
    val a=new EhlConfiguration().addResource("offline-base-data\\..\\offline-base-data\\src\\main\\resources\\base.conf")
    a.foreach()
//    val array = ListBuffer[String]()
//    array +="abc"
//    array +="sdf"
////    array.foreach(println)
//  for(i<-0 to array.size-1){
//    println(i+"-"+(i+1)+"-"+array.size)
//    println(array(i)+"\t"+array((i+1)))
//  }
//    println(DateTime.parse("2016-11-30 00:00:58 000",DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss SSS")).toDate.getTime)
//    println(DateTime.parse("2016-11-30 01:00:58 000",DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss SSS")).toDate.getTime)
//
//    println((1480438858000L-1480435258000L)/1000/60/60)
//    println("2016-11-30 17:00:54 000".substring(0,10))
    val length = "VERSION=1.0,PASSTIME=2016-11-30 16:58:41 000,CARSTATE=1,CARPLATE=鲁RNP768,PLATETYPE=2,SPEED=0,PLATECOLOR=2,LOCATIONID=-1,DEVICEID=-1,DRIVEWAY=7,DRIVEDIR=2,CAPTUREDIR=1,CARCOLOR=1,CARBRAND=99,CARBRANDZW=其它,TGSID=1012,PLATECOORD=2530,1215,2663,1247,CABCOORD=0,0,0,0,IMGID1=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbXR6AAtqFZ4oge0586.jpg,IMGID2=,IMGID3=http://20.0.51.132:9099/image/fastdfs/group20/M00/23/0E/FAAzF1g-lUyAbefZAAAIquqTDOk722.jpg,"
//      .split(",",17).dropRight(1).map(f=>DataBaseFunction.convertToTracker(f))
//    println(length)
  }

}
