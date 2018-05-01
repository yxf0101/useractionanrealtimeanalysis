# useractionanrealtimeanalysis
本项目由两个模块，模块一是用sparksql来统计统计出指定日期范围内的，各个区域的top3热门商品。
  第二个模块是利用sparkstreaming来实现实时的动态黑名单机制：将每天对某个广告点击超过100次的用户拉黑和统计每天各省top3热门广告
 1.第一个模块的技术点是:
   1)计算出来每个区域下每个商品的点击次数，group by area, product_id；保留每个区域的城市名称列表；
     自定义UDAF，group_concat_distinct()函数，聚合出来一个city_names字段
   2)join商品明细表，hive（product_id、product_name、extend_info），extend_info是json类型，自定义UDF，get_json_object()函数，
     取出其中的product_status字段，if()函数（Spark SQL内置函数），判断，0 自营，1 第三方；
   3)开窗函数，根据area来聚合，获取每个area下，click_count排名前3的product信息
   4)Spark SQL的数据倾斜解决方案？双重group by、随机key以及扩容表（自定义UDF函数，random_key()）、Spark SQL内置的reduce join转换为map join、      提高shuffle并行度
 2.第二个模块技术点:
    1、实现实时的动态黑名单机制：将每天对某个广告点击超过100次的用户拉黑
    2、基于黑名单的非法广告点击流量过滤机制：这期间对于数据的中间结果我们采用hbase来存储，hbase的timestamp的多个版本，而且它不却分insert和update，        统一就是去对某个行键rowkey去做更新
    3、每天各省各城市各广告的点击流量实时统计：使用updateStateByKey操作，实时计算每天各省各城市各广告的点击量，并时候更新到MySQL
    4、统计每天各省top3热门广告
    5、统计各广告最近1小时内的点击量趋势：各广告最近1小时内各分钟的点击量，使用window操作，对最近1小时滑动窗口内的数据，计算出各广告各分钟的点击量，并       更新到MySQL中





