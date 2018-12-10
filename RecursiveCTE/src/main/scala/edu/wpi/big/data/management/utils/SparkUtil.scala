package edu.wpi.big.data.management.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkUtil {
  private[this] var _conf: SparkConf = _
  private[this] var _sparkContext: SparkContext = _
  private[this] var _sqlContext: SQLContext = _

  System.setProperty("hadoop.home.dir", "C:\\winutil\\")

  private[this] def disableLogs(): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

  private[this] def initializeSparkContext(): Unit = {
    disableLogs()

    if (_sparkContext == null) {
      if (_conf == null)
        throw new Exception("SparkConf not specified!")

      _sparkContext = new SparkContext(_conf)
    }
  }

  private[this] def initializeSQLContext(): Unit = {
    if (_sqlContext == null) {
      if (_sparkContext == null)
        initializeSparkContext()
    }

    _sqlContext = new SQLContext(_sparkContext)
  }

  def conf: SparkConf = _conf

  def conf_=(conf: SparkConf): Unit = {
    _conf = conf
  }

  def addConf(key: String, value: String): Unit = {
    _conf.set(key, value)
  }

  def sparkContext: SparkContext = {
    initializeSparkContext()
    _sparkContext
  }

  def sqlContext: SQLContext = {
    initializeSQLContext()
    _sqlContext
  }
}
