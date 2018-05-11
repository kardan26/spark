package pl.kardan.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

trait BaseSpark {
  def initSpark(name: String) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(name)
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    sc
  }
}
