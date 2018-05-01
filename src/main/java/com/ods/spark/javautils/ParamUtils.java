package com.ods.spark.javautils;

/*import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;*/
import com.ods.spark.exception.ParameterException;
import org.json.JSONArray;

/**
 * 参数工具类
 * @author Administrator
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args) {
		try {
			if(args != null && args.length > 0) {
				return Long.valueOf(args[0]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}  
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(org.json.JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.length()>0) {
			return jsonArray.getString(0);
		}
		return null;
	}




	/**
	 * 获得json某一关键字对应的一个值
	 * 例如{"key":["value1"]},返回String类型的value1
	 *
	 * @param json
	 * @param key
	 * @return
	 * @throws ParameterException
	 */
	public static String getSingleValue(org.json.JSONObject json, String key) throws ParameterException {
		//输入的key非法
		if(key==null || key.equals("")) throw new ParameterException();
		//处理key不存在的异常
		try{
			org.json.JSONArray jsonArray=json.getJSONArray(key);
			if(jsonArray!=null && json.length()>0){
				return jsonArray.getString(0);
			}
		}catch (Exception e){
			System.out.println(key+"not exist");
		}
		return null;
	}
	
}
