package com.ehl.offline.metrics

import com.codahale.metrics.MetricRegistry
import com.ehl.offline.common.EhlConfiguration
import com.ehl.offline.metrics.sink.Sink
import com.ehl.offline.metrics.source.Source
import org.slf4j.LoggerFactory

import scala.collection.mutable
/**
  * Created by 雷晓武 on 2017/1/6.
  */
class EhlMetricsSystem(securityMgr: SecurityManager) extends Serializable{
  private var running: Boolean = false
  private val logger = LoggerFactory.getLogger(getClass)

  private val sinks = new mutable.ArrayBuffer[Sink]
  private val sources = new mutable.ArrayBuffer[Source]
  private val registry = new MetricRegistry()

  private val metricsConf = new MetricsConf

  def registerSources() = {
    for(k <- metricsConf.defaultSource){
      registerSource(Class.forName(k).newInstance().asInstanceOf[Source])
    }
  }


  def registerSinks() = {
    for(k <- metricsConf.defaultSink){
//      sinks += Class.forName(k).getConstructor(classOf[EhlConfiguration],classOf[]).newInstance().asInstanceOf[Sink]
    }
  }

  def registerSource(source: Source) {
    sources += source
    try {
      registry.register(source.sourceName, source.metricRegistry)
    } catch {
      case e: IllegalArgumentException => logger.info("Metrics already registered", e)
    }
  }


  def start={
    require(!running, "Attempting to start a MetricsSystem that is already running")
    running = true
    registerSources()
    registerSinks()
    sinks.foreach(_.start)
  }

  def stop={
    if (running) {
      sinks.foreach(_.stop)
    } else {
      logger.info("Stopping a MetricsSystem that is not running")
    }
    running = false
  }

  def report={
    sinks.foreach(_.report())
  }

}

class MetricsConf {
  val defaultSource = Array("com.ehl.offline.metrics.source.JvmSource")

  val defaultSink = Array("com.ehl.offline.metrics.sink.ConsoleSink")

  val conf = {
    val c = System.getProperty("METRICS_CONF", "metrics.conf")
    new EhlConfiguration().addResource(c)
  }
}
  /**
    *
    */

object EhlMetricsSystem{
    /**
      *
      * @param instance
      * @param conf
      * @param securityMgr
      * @return
      */
    def createMetricsSystem(instance: String, conf: EhlConfiguration, securityMgr: SecurityManager): EhlMetricsSystem = {
      new EhlMetricsSystem(securityMgr)
    }

    def createMetricsSystem=new EhlMetricsSystem(new SecurityManager)
}

