package com.ods.spark.service

import java.util
import com.ods.spark.conf.ConfManager
import com.ods.spark.constant.Constants
import com.ods.spark.daofactory.DaoFactory
import com.ods.spark.domain.AreaTop3Product
import com.ods.spark.javautils.ParamUtils
import com.ods.spark.scalautils.{InitUtils, ParseJsonUtils}
import com.ods.spark.udaf.GroupConcatDistinctUDAF
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, RowFactory, SparkSession}
import org.json.JSONObject


object AreaTop3Commodity {
              val myUDF1=(v1:Long,v2:String,split:String)=>{
                        v1.toString+split+v2
              }
              val myUDF2=(json:String,field:String)=>{
                   val jsonObject=new JSONObject(json)
                   try{
                     ParseJsonUtils.getString(jsonObject,field)
                   }catch{
                     case e:Exception=>e.getStackTrace
                   }
                   Nil
  }
  def main(args: Array[String]): Unit = {
            // val sc=InitUtils.initSparkContext()._1
             val sparkSession=InitUtils.getSqlContext()
             //获取将j2ee传过来的任务id，对于这一块的代码看我的任务监控项目：https://github.com/yxf0101/spark-job-monitor
             val taskId=ParamUtils.getTaskIdFromArgs(args)
             //通过taskId在mysql里查询任务json
             val task=DaoFactory.getTaskDao.findById(taskId)
             //获取任务参数
             val taskParam=new JSONObject(task.getTaskParam)
             val params = ParseJsonUtils.getParams(taskParam)
             val startDate = params._1
             val endDate = params._2

              sparkSession.udf.register("concat_long_string",myUDF1)
              sparkSession.udf.register("get_json_object",myUDF2)
              sparkSession.udf.register("group_concat_distinct",new GroupConcatDistinctUDAF)
             //查询用户指定日期范围内的点击行为数据
             val clickActionRDD=getClickActionRDDByDate(startDate,endDate,sparkSession)
             val cityInfoRDD=getCityInfoRDD(sparkSession)
      //          cityInfoRDD.foreach(t=>println(t._2.getString(2)))
             generateClickProductBasicTable(cityInfoRDD,clickActionRDD,sparkSession)
             generateTempAreaProductClickTable(sparkSession)
             generateTempAreaFullProductClickCountTable(sparkSession)
             val areaTop3ProductRDD=getAreaTop3ProductRDD(sparkSession)
             val rows=areaTop3ProductRDD.collect()
             writeAreaTop3ProductToMysql(taskId,rows)
             sparkSession.stop()
  }

  /**
    * 查询指定日期范围内的点击行为数据
    * @param startDate
    * @param endDate
    * @return
    */
  private def getClickActionRDDByDate(startDate:String,endDate:String,sparkSession: SparkSession):RDD[(Long,Row)]={
           //从hive的user_visit_action中查询用户访问行为数据
            sparkSession.sql("DROP TABLE IF EXISTS user_visit_action")
            sparkSession.table("user_visit_action").createOrReplaceTempView("user_visit_action_hive")
            val sql = "select city_id,product_id,click_product_id" +
                        "from user_visit_action_hive" +
                         "where click_product_id is not null" + "and click_product_id!='null'"+
                              "and click_product_id!='NULL'" + "and action_time>='" + startDate + "'" +
                              "and action_time<='" + endDate + "'"
            val clickActionRDD=sparkSession.sql(sql).rdd.map(row=>(row.getLong(0),row))
                    clickActionRDD
  }
  private def getProductInfo(sparkSession: SparkSession): Unit ={
           sparkSession.sql("DROP TABLE IF EXISTS hive_product_info")
           sparkSession.table("product_info").createOrReplaceTempView("hive_product_info")

  }
  private def getCityInfoRDD(sparkSession: SparkSession):RDD[(Long,Row)] ={
         //构建MYSQL连接配置信息
           var url=""
           val local=ConfManager.getProperty(Constants.SPARK_LOCAL).toBoolean
           if(local)
            url=ConfManager.getProperty(Constants.JDBC_URL)
           else
            url=ConfManager.getProperty(Constants.JDBC_URL_PROD)
           val cityInfoRDD = sparkSession.read.format("jdbc")
           .option("url",url)
           .option("user", ConfManager.getProperty(Constants.JDBC_USER))
           .option("password", ConfManager.getProperty(Constants.JDBC_PASSWORD))
           .option("dbtable", "city_info").load().rdd.map(row=>(row.getInt(0).toLong,row))
             cityInfoRDD

  }
  private def generateClickProductBasicTable(cityInfoRDD:RDD[(Long,Row)],clickActionRDD:RDD[(Long,Row)],
                                       sparkSession: SparkSession): Unit ={
            val mapRDD=clickActionRDD.join(cityInfoRDD).map(t=>{
            val cityId=t._1
            val clickActionRow=t._2._1
            val cityInfoRow=t._2._2
            val productId=clickActionRow.getLong(1)
            val cityName=cityInfoRow.getString(1)
            val area=cityInfoRow.getString(2)
            Row(cityId,cityName,area,productId)
           // RowFactory.create(cityId,cityName,area,productId)
       })
             val structType=StructType(Array(StructField("city_id",LongType,true),
                                             StructField("city_name",StringType,true),
                                             StructField("area",StringType,true),
                                             StructField("product_id",LongType,true)))
             val df=sparkSession.createDataFrame(mapRDD,structType)
             df.createOrReplaceTempView("tmp_click_prod_basic")
  }
  private def generateTempAreaProductClickTable(sparkSession: SparkSession) = {
              val sql="select " +
                        "area," +
                        "product_id," +
                        "count(*) click_count, "+
                        "group_concat_distinct(concat_long_string(cityid,cityname,':')) city_infos " +
                    "from tmp_click_prod_basic " +
                        "group by area,product_id"
              val df=sparkSession.sql(sql)
              df.createOrReplaceTempView("tmp_area_prod_click_count")
  }
  private def generateTempAreaFullProductClickCountTable(sparkSession: SparkSession): Unit ={
              val sql =
                "SELECT " +
                  "tapcc.area," +
                  "tapcc.product_id," +
                  "tapcc.click_count," +
                  "tapcc.city_infos," +
                  "pi.product_name," +
                  "if(get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品') product_status " +
                  "FROM tmp_area_product_click_count tapcc " +
                  "JOIN hive_product_info pi ON tapcc.product_id=pi.product_id "
              val df = sparkSession.sql(sql)
                  df.createOrReplaceTempView("tmp_area_fullprod_click_count")
  }

  /**
    * 获取各区域top3热门商品
    * @param sparkSession
    * @return
    */
  private def getAreaTop3ProductRDD(sparkSession: SparkSession):RDD[Row]={
              val sql =
                    "SELECT " +
                        "area," +
                        "CASE " +
                             "WHEN area='华北' OR area='华东' THEN 'A级' " +
                             "WHEN area='华南' OR area='华中' THEN 'B级' " +
                             "WHEN area='西北' OR area='西南' THEN 'C级' " +
                             "ELSE 'D级' " +
                             "END area_level," +
                             "product_id," +
                             "click_count," +
                             "city_infos," +
                             "product_name," +
                             "product_status " +
                             "FROM ("+
                                "SELECT " +
                                    "area," +
                                    "product_id," +
                                    "click_count," +
                                    "city_infos," +
                                    "product_name," +
                                    "product_status," +
                                    "ROW_NUMBER() OVER(PARTITION BY area ORDER BY click_count DESC) rank " +
                                    "FROM tmp_area_fullprod_click_count " +
                                    ") t " +
                                "WHERE rank<=3"
                        sparkSession.sql(sql).rdd
  }
  /**
    * 将计算出来的各区域top3热门商品写入MySQL中
    * @param rows
    */
  private def writeAreaTop3ProductToMysql(taskId:Long,rows:Array[Row]): Unit ={
              val areaTop3Products=new util.ArrayList[AreaTop3Product]()
                for(row<-rows){
                  val areaTop3Product = new AreaTop3Product()
                  areaTop3Product.setTaskid(taskId)
                  areaTop3Product.setArea(row.getString(0))
                  areaTop3Product.setAreaLevel(row.getString(1))
                  areaTop3Product.setProductid(row.getLong(2))
                  areaTop3Product.setClickCount(row.getLong(3))
                  areaTop3Product.setCityInfos(row.getString(4))
                  areaTop3Product.setProductName(row.getString(5))
                  areaTop3Product.setProductStatus(row.getString(6))
                  areaTop3Products.add(areaTop3Product)
                }
                val areTop3ProductDAO = DaoFactory.getAreaTop3ProductDAO
                    areTop3ProductDAO.insertBatch(areaTop3Products)
  }
}
