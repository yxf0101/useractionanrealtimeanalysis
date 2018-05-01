package com.ods.spark.constant;

public interface Constants {
   String JDBC_DRIVER="jdbc.driver";
   String JDBC_DATASOURCE_SIZE="jdbc.datasource.size";
   String JDBC_URL="jdbc.url";
   String JDBC_USER="jdbc.user";
   String JDBC_PASSWORD="jdbc.password";
   String JDBC_URL_PROD = "jdbc.url.prod";
   String JDBC_USER_PROD = "jdbc.user.prod";
   String JDBC_PASSWORD_PROD = "jdbc.password.prod";






   String SPARK_APP_NAME_SESSION="AreaTop3ProductSpark";
   String SPARK_LOCAL="spark.local";
   String SPARK_MASTER="local[3]";
   String SPARK_SQL_WAREHOURSR_DIR="spark.sql.warehouse.dir";
   String SPARK_STREAMING_BATCH_INTERVAL="spark.streaming.batch.interval";

   String PARAM_START_DATE="startDate";
   String PARAM_END_DATE="endDate";

   String KAFKA_METADATA_BROKER_LIST="kafka.metadata.broker.list";
   String KAFKA_TOPICS="kafka.topics";




}
