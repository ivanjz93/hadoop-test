# hadoop-test
hadoop、mapreduce的一些练习

- 包org.dan.service	Hadoop RPC练习
- 包org.dan.mr.wordcount	MapReduce单词计数
- 包org.dan.mr.flowsum	MapReduce流量统计
- 包org.dan.mr.flowsumsort	MapReduce流量统计，按总流量排序
- 包org.dan.mr.order_pro	MapReduce实现订单信息和产品信息的join逻辑
- 包org.dan.mr.order_pro_mapjoin	MapReduce实现订单信息和产品信息的join逻辑，在Mapper端实现，避免数据倾斜
- 包org.dan.mr.wordindex	MapReduce单词索引
- 包org.dan.mr.shared_friends	MapReduce查找共同好友
- 包org.dan.mr.max_order_price	MapReduce编写自定义的Partitioner和GroupingComparator实现高效求最大值
- 包org.dan.mr.smallfile	MapReduce自定义FileInputFormat合并小文件
- 包org.dan.hive.udf	hive自定义udf函数：字符串转换为小写、代码和地区映射和解析json
- 包org.dan.mr.accesslog 将hdfs中 tomcat accesslog 转换为为贴源层数据保存在hdfs中
- 包org.dan.mr.pageview 将贴源层数据处理为pageview数据
- 包org.dan.mr.clickstream 将pageview数据处理为点击流数据

config文件夹：

- tail-tomcat.sh脚本，监视tomcat的access log的输出
- tail-hdfs.conf flume配置文件，将tail-tomcat.sh脚本输出的内容保存到hdfs中
- mvdata.sh 将flume采集到hdfs的文件统一移动到一个预处理文件夹
- create_table.sql 创建贴源层、ageview和点击流hive数据表
- load_ods_table.sh 导入三个表的数据