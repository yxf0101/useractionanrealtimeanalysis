# useractionanrealtimeanalysis
本项目由两个模块，模块一是用sparksql来统计统计出指定日期范围内的，各个区域的top3热门商品。
  第二个模块是利用sparkstreaming来实现实时的动态黑名单机制：将每天对某个广告点击超过100次的用户拉黑和统计每天各省top3热门广告
 1.第一个模块的技术点是:
   1)计算出来每个区域下每个商品的点击次数，group by area, product_id；保留每个区域的城市名称列表；
     自定义UDAF，group_concat_distinct()函数，聚合出来一个city_names字段
   2)join商品明细表，hive（product_id、product_name、extend_info），extend_info是json类型，自定义UDF，get_json_object()函数，
     取出其中的product_status字段，if()函数（Spark SQL内置函数），判断，0 自营，1 第三方；

