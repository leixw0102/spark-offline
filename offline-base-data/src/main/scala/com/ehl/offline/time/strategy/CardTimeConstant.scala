package com.ehl.offline.time.strategy

/**
  * Created by 雷晓武 on 2017/1/11.
  */
trait CardTimeConstant {
  val basePathKey = "time.avg.input.path"
  val inputFromKey = "time.avg.input.from"
  val inputSetpSize="time.avg.input.size"
  val maxTimeKey = "time.max.time"

  val downFloor="time.avg.downfloor"
  val upFloor="time.avg.upfloor"
}
