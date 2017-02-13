package com.ehl.offline.core.parser

/**
  * Created by 雷晓武 on 2017/1/12.
  * 解析shell传递参数
  */
trait ArgumentParser {
    def parser(args:Array[String]):Unit
}
