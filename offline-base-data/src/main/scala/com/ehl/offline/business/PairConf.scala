package com.ehl.offline.business

/**
  * Created by 雷晓武 on 2016/12/7.
  * 浮动参数
  */
class PairConf {
    val startH=1
    val endH=5
}

object PairConf{
  private val singleton = new PairConf
  def getStartH=singleton.startH
  def getEndH=singleton.endH
}
