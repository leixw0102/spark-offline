package com.ehl.offline

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

import com.ehl.hdfs.HdfsUtils
import com.ehl.offline.common.EhlConfiguration
import org.joda.time.DateTime

import scala.Option
import scala.util.matching.Regex

/**
  * Created by 雷晓武 on 2017/2/15.
  */
object FileUnionAction extends App{


  val file = System.getProperty("mobile-config","base.conf")
  val conf = new EhlConfiguration().addResource(file)

  val path = Paths.get(conf.get("ftp.directory.path"))


  val backDir = path.resolve(Paths.get(conf.get("ftp.back.directory.name","back")))

  val hdfsUri = conf.get("mobile.hdfs.path","hdfs://cluster1/app/mobile/")

  if(java.nio.file.Files.notExists(backDir)) {java.nio.file.Files.createDirectory(backDir)}
  require(java.nio.file.Files.exists(path))

  //directory tree
  java.nio.file.Files.walkFileTree(path,new FindFileVisitor(path,backDir))

  java.nio.file.Files.walkFileTree(path,new SimpleFileVisitor[Path]{
    val dateString = DateTime.now.toString("yyyyMMdd")
    override def postVisitDirectory(p:Path,exc:IOException): FileVisitResult={
      if(p.toString.startsWith(backDir.toString) ){
//        println(p.toString+"---")
      }else {

        pattern1.findFirstMatchIn(p.getFileName.toString) match {
          case Some(file) => {

//            println(file +"++++"+dateString)
            if (file.toString().equals(dateString)) {
              println("current day ,then no sender to hdfs")
            } else {
              try {
                HdfsUtils.sendDirectory(new org.apache.hadoop.fs.Path(p.toString), new org.apache.hadoop.fs.Path(hdfsUri))
                java.nio.file.Files.move(p, backDir.resolve(p.getFileName))
              } catch {
                case e: Exception => e.printStackTrace()
              } finally {

              }
              println("send to hdfs and mv to back directory" + p.toString + "\t" + backDir.resolve(p.getFileName))

            }
          }
          case None => println("no match")
        }
      }
      FileVisitResult.CONTINUE
    }
    val pattern1 = new Regex("""\d{8}""")
    override def preVisitDirectory(p:Path,attrs:BasicFileAttributes):FileVisitResult ={

      return FileVisitResult.CONTINUE

    }
  })

}



class FindFileVisitor(base:Path,back:Path) extends SimpleFileVisitor[Path]{
  val yesterday=new DateTime().plusDays(-1).toString("yyyyMMdd")
  val yesterday_directory_prefix=yesterday+"_"
  val pattern = new Regex(""".*(\d{8})\d{6}_\d{10}.\d{4}.dat""","g")
  override def preVisitDirectory(p:Path,attributes: BasicFileAttributes)={
    if(p.getFileName.toString.equals(back.getFileName.toString)){

      FileVisitResult.SKIP_SUBTREE
    }else{
      FileVisitResult.CONTINUE
    }
//    FileVisitResult.CONTINUE

  }
  override def visitFile(p:Path,attrs:BasicFileAttributes) ={
    pattern.findFirstMatchIn(p.getFileName.toString)match {
      case Some(date)=>{

//        val dir = base.resolve(date.group("g"))
        val dir = yesterday_directory_prefix+date.group("g");
        //add dir rule
        /**
          * yesterday_{0}
          * {0}--:pattern  p.getFileName  to date
          */
        val dir_rule=Paths.get(base.resolve(dir).toString)
        if(java.nio.file.Files.notExists(dir_rule)){
          java.nio.file.Files.createDirectory(dir_rule)
        }
        java.nio.file.Files.move(p,dir_rule.resolve(p.getFileName),StandardCopyOption.REPLACE_EXISTING)
//        println(s"mv source ${p.toString} to target ${dir_rule.toString}")
      }
      case None=>println("the false of the matching result ;the pattern ="+pattern+" and the source ="+p.getFileName)
    }

    FileVisitResult.CONTINUE
  }
}
