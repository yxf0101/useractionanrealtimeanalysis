package com.ods.sparktest

import com.ods.spark.conf.ConfManager
import com.ods.spark.constant.Constants
import com.ods.spark.scalautils.InitUtils

import org.apache.spark.sql.SparkSession
import org.junit.{After, Before, Test}

class SparkSqlTest {
        private val paymentFile = "src/test/resources/payment.csv"
        private val clickFile = "src/test/resources/click.log"
        private var sparkSession: SparkSession =_

  @Before
  def initiate: Unit = {
        sparkSession = SparkSession.builder().appName("SparkSqlTest")
          .master("local[2]")
          .config(Constants.SPARK_SQL_WAREHOURSR_DIR,ConfManager.getProperty(Constants.SPARK_SQL_WAREHOURSR_DIR))
          .enableHiveSupport()
          .getOrCreate()
  }

  @After
  def close: Unit = {
        sparkSession.stop()
  }

  @Test
  def test01(): Unit = {
        val df = sparkSession.read.option("headr", true).csv(paymentFile)

        df.createOrReplaceTempView("payment")
        df.sqlContext.sql("select _c1 from payment").show()
  }

  @Test
  def test02(): Unit = {
        val df = sparkSession.read.format("jdbc")
                  .option("url", ConfManager.getProperty(Constants.JDBC_URL))
                  .option("user", ConfManager.getProperty(Constants.JDBC_USER))
                  .option("password", ConfManager.getProperty(Constants.JDBC_PASSWORD))
                  .option("dbtable", "tb_user").load()
                  df.createOrReplaceTempView("user")
                  df.sqlContext.sql("select username,password from user").show(10)

  }

  @Test
  def test(): Unit = {
          // import sqlContext.implicits._
          val clickActionRDD = sparkSession.sparkContext.textFile(clickFile).map(_.split(" "))
                .map(arr => clickAction(arr(0).trim, arr(1).toLong, arr(2).trim, arr(3).toLong,
                arr(4).trim, arr(5).trim, arr(6).trim, arr(7).trim, arr(8).trim,
                arr(9).trim, arr(10).trim, arr(11).trim, arr(12).trim, arr(13).toLong))
          val clickActionDF = sparkSession.sqlContext.createDataFrame(clickActionRDD)
                clickActionDF.createOrReplaceTempView("user_visit_action")


          val startDate = "2017-03-06"
          val endDate = "2017-03-06"
          val sql="select * from user_visit_action"
          clickActionDF.sqlContext.sql(sql).show(10)
  }
   @Test
  def test03(): Unit ={
         //从hive的users中查询用户访问行为数据
         // sparkSession.sql("DROP TABLE IF EXISTS users")
         sparkSession.table("yang.users").write.saveAsTable("user_tab")
         sparkSession.sql("select * from user_tab").show()



  }
}
case class clickAction(date:String,user_id:Long,session_id:String,page_id:Long, action_date:String,
         action_time:String,search_keyword:String,click_category_id:String,
         click_product_id:String,order_category_ids:String,order_product_ids:String,
         pay_category_ids:String,pay_product_ids:String,city_id:Long) extends Serializable
