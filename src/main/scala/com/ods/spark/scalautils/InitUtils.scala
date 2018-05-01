package com.ods.spark.scalautils

import com.ods.spark.conf.ConfManager
import com.ods.spark.constant.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


                /**
                  * 初始化spark环境
                  */
object InitUtils {
                  //初始化SparkConf
                  val conf=getSparkConf()
                  val sc=new SparkContext(conf)
  def initSparkContext():(SparkContext,StreamingContext)={
                 val streamingContext=getSparkStreamingContext(sc)
                 Logger.getRootLogger.setLevel(Level.OFF)
                 (sc,streamingContext)
  }
  def getSparkConf():SparkConf={
                val local=ConfManager.getBoolean(Constants.SPARK_LOCAL)
                if(local)
                  new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster(Constants.SPARK_MASTER)
                                   //.set("spark.driver.allowMultipleContexts","true")
                else
                  new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
  }

  /**
    * 对SparkSession初始化，如果是是本地测试，读取本地数据，如果是集群测试，读取hive表数据
    * @return
    */
  def getSqlContext():SparkSession={
                      val local=ConfManager.getBoolean(Constants.SPARK_LOCAL)
                if(local)
                      SparkSession.builder().appName("ActionOperation")
                      .master("local[2]")
                      .config(Constants.SPARK_SQL_WAREHOURSR_DIR,ConfManager.getProperty(Constants.SPARK_SQL_WAREHOURSR_DIR))
                      .getOrCreate()
                else
                      SparkSession.builder().appName("ActionOperation")
                      .config(Constants.SPARK_SQL_WAREHOURSR_DIR,ConfManager.getProperty(Constants.SPARK_SQL_WAREHOURSR_DIR))
                      .enableHiveSupport()
                      .getOrCreate()

  }
  def getSparkStreamingContext(sc:SparkContext):StreamingContext={
                    val ssc = new StreamingContext(sc, Seconds(ConfManager.getLong(Constants.SPARK_STREAMING_BATCH_INTERVAL)))
                    ssc
  }
}
