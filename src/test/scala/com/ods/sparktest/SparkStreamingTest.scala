package com.ods.sparktest


import com.ods.spark.scalautils.InitUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.junit.{After, Before, Test}

class SparkStreamingTest {
    private var ssc:StreamingContext= _
  @Before
  def initiate: Unit = {
       ssc=InitUtils.initSparkContext()._2
  }
  @Test
  def writeToHbasetest(): Unit ={
                /**
                  * 一条一条的实时日志
                  * timestamp province city userid adid
                  *
                  */
                 val adRealTimeLogDStream=KafkaUtils.createStream(ssc,"mot1:2181,mot2:2181,mot3:2181","group1",Map("parks"->1)).map(_._2)
                 adRealTimeLogDStream.print()
                 val dailyUserAdClickDStream=adRealTimeLogDStream.map(_.split(" ")).map(arr=>{
                 //提取出日期yyyyMMdd、userid、adid
                 val timestamp=arr(0)
                 val userId=arr(3).toLong
                 val adId=arr(4).toLong
                  (timestamp,userId,adId)})
                  dailyUserAdClickDStream.foreachRDD(saveToHbase(_))
                  ssc.start()
                  ssc.awaitTermination()
                  ssc.stop()

  }
      @Test
      def readHbaseData(): Unit ={
                 //2.指定配置信息
                 val confx = HBaseConfiguration.create()
                 //--zk地址
                 confx.set("hbase.zookeeper.quorum", "mot1,mot2,mot3")
                 //--zk端口
                 confx.set("hbase.zookeeper.property.clientPort", "2181")
                 //--操作的表
                 confx.set(TableInputFormat.INPUT_TABLE, "tbs")

                  //3.读取hbase
                 val hBaseRDD = ssc.sparkContext.newAPIHadoopRDD(confx, classOf[TableInputFormat],
                    classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                    classOf[org.apache.hadoop.hbase.client.Result])
                 hBaseRDD.foreach{
                   case(_,result)=>{
                       val rowKey=Bytes.toString(result.getRow)
                       val timestamp=Bytes.toString(result.getValue("cf1".getBytes(),"timestamp".getBytes()))
                       val userId=Bytes.toString(result.getValue("cf1".getBytes(),"userId".getBytes()))
                       val adId=Bytes.toString(result.getValue("cf1".getBytes(),"adId".getBytes()))
                       println("Row key:"+rowKey+" timestamp:"+timestamp+" userId:"+userId+" adId:"+adId)

                   }
                 }
                   ssc.stop()


     }
       def saveToHbase(dataRDD:RDD[(String,Long,Long)]): Unit ={
                 ssc.sparkContext.hadoopConfiguration.set("hbase.zookeeper.quorum", "mot1,mot2,mot3")
                 ssc.sparkContext.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
                 ssc.sparkContext.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "tbs")
                 //--创建conf对象
                 val job = new Job(ssc.sparkContext.hadoopConfiguration)
                 job.setOutputKeyClass(classOf[ImmutableBytesWritable])
                 job.setOutputValueClass(classOf[Result])
                 job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
                 val confx = job.getConfiguration()
                 dataRDD.map(tuple=>{
                 val put = new Put(Bytes.toBytes(tuple._1+"_"+tuple._2))
                 put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("timestamp"),Bytes.toBytes(tuple._1))
                 put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("userId"),Bytes.toBytes(tuple._2.toLong))
                 put.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("adId"),Bytes.toBytes(tuple._3.toLong))
                 (new ImmutableBytesWritable, put)
                 }).saveAsNewAPIHadoopDataset(confx)
       }
}














