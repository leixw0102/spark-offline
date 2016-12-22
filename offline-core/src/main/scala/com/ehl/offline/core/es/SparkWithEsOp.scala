

/**
 * @author ehl
 */
package com.ehl.offline.core.es

import com.ehl.offline.common.EhlConfiguration
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
 *
 * @author ehl
 */
trait SparkWithEsOp{



  def setESConfig(sc:SparkConf,ehlConfiguration: EhlConfiguration):Unit

}