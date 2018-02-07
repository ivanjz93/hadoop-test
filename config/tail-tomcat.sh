datename=$(date +%Y-%m-%d)             #定义时间变量名和显示时间格式
#echo $datename
tail -f /opt/apache-tomcat-7.0.73/logs/localhost_access_log.$datename.txt
