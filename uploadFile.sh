#!/bin/bash

#set java env
export JAVA_HOME=/home/hadoop/app/jdk1.7.0_51
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH

#set hadoop env
export HADOOP_HOME=/home/hadoop/app/hadoop-2.6.4
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH

#日志文件存放目录
log_src_dir=/home/hadoop/logs/log/

#待上传文件的存放目录
log_toupload_dir=/home/hadoop/logs/toupload/

#日志文件上传到hdfs的路径
hdfs_root_dir=/data/clickLog/20171212/

#打印环境变量
echo "envs: hadoop_home: $HADOOP_HOME"

#读取日志文件的目录，判定是否有需要上传的文件
echo "log_src_dir:"$log_src_dir
ls $log_src_dir | while read fileName
do 
	if [[ "$fileName" == access.log.* ]]; then
		date = 'date +%Y %m %d %H %M %S'
		#将文件移动到待上传目录并重命名
		#打印信息
		echo "moving $log_src_dir$fileName to $log_toupload_dir"xxxxxx_click_log_$fileName"$date"
		mv $log_src_dir$fileName $log_toupload_dir"xxxxxx_click_log_$fileName"$date
		#将待上传的文件path写入一个列表文件willDoing
		echo $log_toupload_dir"xxxxxx_click_log_"$date >> $log_toupload_dir"willDoing."$date
	fi
done

#找到列表文件willDoing
ls $log_toupload_dir | grep will | grep -v "_COPY_" | grep -v "_DONE_" | while read line
do
	#打印信息
	echo "toupload is in file:"$line
	#将待上传文件列表willDoing改名为willDoing_COPY_
	mv $log_toupload_dir$line $log_toupload_dir$line"_COPY_"
	#读取列表文件willDoing_COPY_的内容（一个一个的待上传文件名），此处的line就是列表中的一个待上传文件的path
	cat $log_toupload_dir$line"_COPY_" | while read line
	do 
		echo "putting...$line to hdfs path......$hdfs_root_dir"
		hadoop fs -put $line $hdfs_root_dir
	done
	mv $log_toupload_dir$line"_COPY_" $log_toupload_dir$line"_DONE_"
done