export JAVA_HOME=/usr/lib/jvm/java-8-oracle
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH

export HADOOP_HOME=/home/ctbri/jz/hadoop-2.9.0
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH

log_flume_dir=/flume/tomcat-access/
log_pre_input=/tomcat-access/input

day_01=`date +%y-%m-%d`
syear=`date +%y`
smonth=`date +%m`
sday=`date +%d`

files=`hadoop fs -ls $log_flume_dir | grep $day_01 | wc -l`
if [ $files -gt 0 ]; then
	for line in `hadoop fs -ls -C $log_flume_dir/${day_01}`
	do
		hadoop fs -mv $line/* ${log_pre_input}
	done
fi