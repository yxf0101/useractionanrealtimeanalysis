package com.ods.spark.service

import java.util
import java.util.{Date, Random}

import com.ods.spark.conf.ConfManager
import com.ods.spark.constant.Constants
import com.ods.spark.daofactory.DaoFactory
import com.ods.spark.domain.AdBlacklist
import com.ods.spark.javautils.DateUtils
import com.ods.spark.scalautils.InitUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{RegexStringComparator, RowFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Minutes, Seconds}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object AdRealTimeClickSpark {
                  val sc=InitUtils.initSparkContext()._1
                  val sparkSession=InitUtils.getSqlContext()
  def main(args: Array[String]): Unit = {
                  val ssc=InitUtils.initSparkContext()._2
                      ssc.checkpoint("hdfs://mot1:9000/checkpoint/update_result")
                  val topicMap=Map("adRealInfo"->1,"advert"->2)
                  val kafkaBrokerList=ConfManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
                  val adRealTimeLogDStream=KafkaUtils.createStream(ssc,kafkaBrokerList,"group1",topicMap).map(_._2)
                        adRealTimeLogDStream.print()
                  /**
                    * 从kafka中向sparkstreaming传过来的是一条一条的日志，格式是：
                    * timestamp province city userid adid
                    * 计算出没3秒内的数据batch，每天每个用户每条广告的点击量
                    * 通过对原始实时日志的处理，将日志的格式处理成<yyyyMMdd_userid_adid,1L>的格式
                    * 并累加
                    */
                  val dailyUserAdClickCountDStream=adRealTimeLogDStream.map(_.split(" ")).map(arr=>{
                    //提取出日期（yyyyMMdd)、userid、adid
                  val timestamp=arr(0)
                  val date=new Date(timestamp.toLong)
                  val dateKey=DateUtils.formatDateKey(date)
                  val userId=arr(3).toLong
                  val adId=arr(4).toLong
                  val key=dateKey+"_"+userId+"_"+adId
                    (key,1L)}).reduceByKey(_+_)
                  //把数据存入hbase
                  dailyUserAdClickCountDStream.foreachRDD(savaToHbase(_,sc))
                  // 现在我们在hbase里面，已经有了累计的每天各用户对各广告的点击量
                  // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
                  // 从hbase中查询
                  // 查询出来的结果，如果是大于100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
                  // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
                  // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
                  val distinctBlacklistUseridDStream=dailyUserAdClickCountDStream.filter(filterRDD(_,sc))
                      .map(_._1.split("_")(1).toLong).transform(_.distinct())
                  //去重后的,这个rdd就只剩下了userid一个字段了，写入mysql，这就是动态黑名单，以后要根据这来动态过滤非法用户
                  distinctBlacklistUseridDStream.foreachRDD(saveDistinctDataToMysql(_))
                  //根据动态黑名单对实时日志进行过滤
                  val filteredAdRealTimeLogDStream=adRealTimeLogDStream.transform(rdd=>{
                  val adBlacklistDao=DaoFactory.getAdBlacklistDAO
                  val adBlacklists=adBlacklistDao.findAll().toArray
                  val blacklistRDD=sc.parallelize(adBlacklists).map(arr=>(arr.toString.toLong,true))
                  val mapedRDD=rdd.map(s=>{(s.split(" ")(3).toLong,s)})
                        //利用左外连接对这两个rdd进行连接，过滤黑名单用户
                  val joinRDD=mapedRDD.leftOuterJoin(blacklistRDD)
                  val filterRDDs=joinRDD.filter(tuple=>{
                  val optional=tuple._2._2
                  if(optional.isEmpty && !optional.get) true else false
                        })
                  val finalRDD=filterRDDs.map(t=>t._2._1)
                            finalRDD
                  })
                  // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
                  val adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream)
                  //业务功能二:统计计算每天各省份的top3热门广告
                  calculateProvinceTop3Ad(adRealTimeStatDStream)
                  //业务功能三：计算每小时进来的数据点击次数。
                  calculateAdClickCountByWindow(adRealTimeLogDStream)



                ssc.start()
                ssc.awaitTermination()
                ssc.stop()
  }
  private def savaToHbase(dataRDD:RDD[(String,Long)],sc:SparkContext): Unit ={
                sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "mot1,mot2,mot3")
                sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
                sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "adclick")
                //--创建conf对象
                val job = new Job(sc.hadoopConfiguration)
                job.setOutputKeyClass(classOf[ImmutableBytesWritable])
                job.setOutputValueClass(classOf[Result])
                job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
                val confx = job.getConfiguration()
                dataRDD.map(tuple=>{
                val keySplit=tuple._1.split("_")
                   //date 的格式为<yyyy-MM-dd>
                val date=DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
                val userId=keySplit(1).toLong
                val adid=keySplit(2).toLong
                val clickCount=tuple._2
                val put = new Put(Bytes.toBytes(date+"_"+userId+"_"+adid+"_"+"_"+
                             (for(x<-0 to 5)yield{new Random().nextInt(9).toString()}).reduce(_+_)))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("date"),Bytes.toBytes(date))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("userId"),Bytes.toBytes(userId))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("adId"),Bytes.toBytes(adid))
                put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("clickCount"),Bytes.toBytes(clickCount))
                (new ImmutableBytesWritable, put)}).saveAsNewAPIHadoopDataset(confx)



  }
  private def filterRDD(tuple:(String,Long),sc:SparkContext):Boolean={
                val keySplit=tuple._1.split("_")
                val date=DateUtils.formatDate(DateUtils.parseDateKey(keySplit(0)))
                val userId=keySplit(1).toLong
                val adId=keySplit(2).toLong
                val keys=date+"_"+userId+"_"+adId
                val rkRegex="^"+keys+"_.*$"
                val rDD=queryFromHBase(rkRegex,sc)
                if(rDD.filter(_._4>100).count()>0)
                  true
                else
                  false

  }
 private def queryFromHBase(rkRegex:String,sc:SparkContext) = {
                //--指定相关配置
                val conf = HBaseConfiguration.create()
                conf.set("hbase.zookeeper.quorum","mot1,mot2,mot3")
                conf.set("hbase.zookeeper.property.clientPort", "2181")
                conf.set(TableInputFormat.INPUT_TABLE,"adclick")
                //--指定过滤器 根据行键的正则来过滤数据
                if(rkRegex != null){
                val scan = new Scan()
                scan.setFilter(new RowFilter(CompareOp.EQUAL,new RegexStringComparator(rkRegex)))
                conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
                }
                //--查询数据
                val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
                  classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                  classOf[org.apache.hadoop.hbase.client.Result])
                //--将查询到的数据转换成结果rdd 内部为  一个个的tuple,每个tuple代表 一条日志
                val result = hBaseRDD.map{t:(ImmutableBytesWritable, Result)=>{
                  //获取行键
                val rk = Bytes.toString(t._2.getRow)
                  //通过列族和列名获取列
                val date = Bytes.toString(t._2.getValue("cf1".getBytes,"date".getBytes))
                val userId = Bytes.toLong(t._2.getValue("cf1".getBytes,"userId".getBytes))
                val adId = Bytes.toLong(t._2.getValue("cf1".getBytes,"adId".getBytes))
                val clickCount = Bytes.toLong(t._2.getValue("cf1".getBytes,"clickCount".getBytes))
                  (date,userId,adId,clickCount)
                }}
                //--返回结果rdd
                result
  }
  private def saveDistinctDataToMysql(dataRDD:RDD[Long]): Unit = {
               dataRDD.foreachPartition(iter=>{
               val adBlacklists = new util.ArrayList[AdBlacklist]()
               while(iter.hasNext){
                  val userid = iter.next
                  val adBlacklist = new AdBlacklist
                          adBlacklist.setUserid(userid)
                          adBlacklists.add(adBlacklist)
                     }
                  val adBlacklistDAO = DaoFactory.getAdBlacklistDAO()
                     adBlacklistDAO.insertBatch(adBlacklists)
                    })
  }
                    // 业务逻辑一
                    // 广告点击流量实时统计
                    // 上面的黑名单实际上是广告类的实时系统中，比较常见的一种基础的应用
                    // 实际上，我们要实现的业务功能，不是黑名单

                    // 计算每天各省各城市各广告的点击量
                    // 这份数据，实时不断地更新到mysql中的，J2EE系统，是提供实时报表给用户查看的
                    // j2ee系统每隔几秒钟，就从mysql中搂一次最新数据，每次都可能不一样
                    // 设计出来几个维度：日期、省份、城市、广告
                    // j2ee系统就可以非常的灵活
                    // 用户可以看到，实时的数据，比如2018-03-27，历史数据
                    // 2018-03-27，当天，可以看到当天所有的实时数据（动态改变），比如江苏省南京市
                    // 广告可以进行选择（广告主、广告名称、广告类型来筛选一个出来）
                    // 拿着date、province、city、adid，去mysql中查询最新的数据
                    // 等等，基于这几个维度，以及这份动态改变的数据，是可以实现比较灵活的广告点击流量查看的功能的

                    // date province city userid adid
                    // date_province_city_adid，作为key；1作为value
                    // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
                    // 我们要对原始数据进行map，映射成<date_province_city_adid,1>格式
                    // 然后呢，对上述格式的数据，执行updateStateByKey算子
                    // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
  private def calculateRealTimeStat(filteredAdRealTimeLogDStream: DStream[String]):DStream[(String,Long)]={
                    val mappedDStream=filteredAdRealTimeLogDStream.map(_.split("")).map(arr=>{
                    val timestamp = arr(0)
                    val date = new Date(timestamp.toLong)
                    val datekey = DateUtils.formatDateKey(date);	// yyyyMMdd
                    val province = arr(1)
                    val city = arr(2)
                    val adId = arr(4).toLong
                    val key = datekey + "_" + province + "_" + city + "_" + adId
                      (key,1L)
            })
                    // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
                    // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
                    val addFunc = (currClickCountValues: Seq[Long], prevClickCountValueState: Option[Long]) => {
                      // 举例来说
                      // 对于每个key，都会调用一次这个方法
                      // 比如key是<20180327_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
                      // 10个
                      // values，(1,1,1,1,1,1,1,1,1,1)
                      // 首先根据optional判断，之前这个key，是否有对应的状态
                      // 如果说，之前是存在这个状态的，那么就以之前的状态作为起点，进行值的累加
                      //通过Spark内部的reduceByKey按key规约，然后这里传入某key当前批次的Seq/List,再计算当前批次的总和
                    val clickCount = currClickCountValues.sum
                      // 已累加的值
                    val previousCount = prevClickCountValueState.getOrElse(0L) // 返回累加后的结果，是一个Option[Long]类型
                    Some(clickCount+previousCount)
                    }
                    val aggregatedDStream = mappedDStream.updateStateByKey[Long](addFunc)
                      //下来就是把计算结果写入hbase或者mysql中，这里就不在操作，很简单
                      aggregatedDStream
     }

  /**
    * 计算每天各省份的top3热门广告
    * @param adRealTimeStatDStream
    * @return
    */
  private def calculateProvinceTop3Ad(adRealTimeStatDStream:DStream[(String,Long)]):Unit={
                    val rowDStream=adRealTimeStatDStream.transform(rdd=>{
                    val mappedRDD=rdd.map(t=>{
                    val logSplit=t._1.split("_")
                    val datekey=logSplit(0)
                    val date=DateUtils.formatDate(DateUtils.parseDateKey(datekey))
                    val province=logSplit(1)
                    val adId=logSplit(2).toLong
                    val clickCount=t._2
                    val key=date+"_"+province+"_"+adId
                      (key,clickCount)
                    })
                    val dailyAdClickCountByProvinceRDD=mappedRDD.reduceByKey(_+_)
                    // 将dailyAdClickCountByProvinceRDD转换为Dataset
                    // 注册为一张临时表
                    // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告
                    val rowsRDD=dailyAdClickCountByProvinceRDD.map(t=>{
                    val keySplit=t._1.split("_")
                    val dateKey=keySplit(0)
                    val date=DateUtils.formatDate(DateUtils.parseDateKey(dateKey))
                    val province=keySplit(1)
                    val adId=keySplit(2).toLong
                    val clickCount=t._2
                      Row(date,province,adId,clickCount)
                    })
                    val structType=StructType(Array(StructField("date",StringType,true),
                     StructField("province",StringType,true),
                     StructField("adId",StringType,true),
                     StructField("clickCount",LongType,true)))
                    val dailyAdClickCountByProvinceDF=sparkSession.createDataFrame(rowsRDD,structType)
                    // 将dailyAdClickCountByProvinceDF，注册成一张临时表
                    dailyAdClickCountByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov")
                    // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
                    val provinceTop3AdDF=sparkSession.sql(
                      "select " +
                          "date," +
                          "province," +
                          "ad_id," +
                          "click_count " +
                        "from ( " +
                           "select " +
                                 "date," +
                                 "province," +
                                 "ad_id," +
                                 "click_count," +
                                 "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank " +
                            "form tmp_daily_ad_click_count_by_prov " +
                        ") t " +
                        "where rank>=3"
                    )
                           provinceTop3AdDF.rdd
           })
                    // rowDStream
                    // 每次都是刷新出来各个省份最热门的top3广告
                    // 将其中的数据批量更新到MySQL中
                    //写入mysql写的太多了，这里就不写了。
                   /* rowDStream.foreachRDD(s=>s.foreachPartition(

                    ))*/
  }

  /**
    *计算最近1小时滑动窗口内的广告点击趋势
    * @param adRealTimeLogDStream
    */
  def calculateAdClickCountByWindow(adRealTimeLogDStream: DStream[String]) = {
                  //把每条日志映射成<yyyyMMddHHMM_adid,1L>格式
                  val pairDStream=adRealTimeLogDStream.map(_.split(" ")).map(arr=>{
                        val timeMinute=DateUtils.formatTimeMinute(new Date(arr(0).toLong))
                        val adId=arr(4).toLong
                        (timeMinute+"_"+adId,1L)
                  })
                  // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
                  // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
                  // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
                  // 1小时滑动窗口内的广告点击趋势
                  // 点图 / 折线图
                  val aggLogsRDD=pairDStream.reduceByKeyAndWindow((x:Long,y:Long)=>x+y,Minutes(1), Seconds(10))
                  // aggLogsRDD
                  // 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
                  // 各广告，在最近1小时内，各分钟的点击量,最后把每小时个广告的点击行为数据写入mysql
                  /*aggLogsRDD.foreachRDD(s=>s.foreachPartition(
                  ))*/
  }



















}
