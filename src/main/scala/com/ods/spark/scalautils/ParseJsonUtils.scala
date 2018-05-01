package com.ods.spark.scalautils

import com.ods.spark.constant.Constants
import com.ods.spark.javautils.ParamUtils
import org.json.JSONObject

object ParseJsonUtils {
   def getParams(json:JSONObject):(String,String)={
     //解析json，获得用户的查询参数
     val startTime=ParamUtils.getSingleValue(json,Constants.PARAM_START_DATE)
     val endTime = ParamUtils.getSingleValue(json, Constants.PARAM_END_DATE)
     (startTime,endTime)
   }
   def getString(jsonObject:JSONObject,field:String):String={
          ParamUtils.getParam(jsonObject,field)
   }
}
