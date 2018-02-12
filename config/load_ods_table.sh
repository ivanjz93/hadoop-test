day_01=`date +%y-%m-%d`
syear=`date +%y`
smonth=`date +%m`
sday=`date +%d`

log_pre_output="/tomcat-access/output"
click_pvout="/znhw-pageview"
click_vsout="/znhw-clickstream"

ods_weblog_origin="website.ods_weblog_origin"
ods_click_pageviews="website.ods_click_pageviews"
ods_click_visit="website.ods_click_visit"

HQL_orgin="load data inpath '$log_pre_output' into table $ods_weblog_origin partition(datestr='$day_01')"
/home/ctbri/jz/apache-hive-1.2.2-bin/bin/hive -e "$HQL_orgin"

HQL_pvs="load data inpath '$click_pvout/$day_01' into table $ods_click_pageviews partition(datestr='$day_01)'"
/home/ctbri/jz/apache-hive-1.2.2-bin/bin/hive -e "$HQL_pvs"

HQL_vst="load data inpath '$click_vsout/$day_01' into table $ods_click_visit partition(datestr='$day_01')"
/home/ctbri/jz/apache-hive-1.2.2-bin/bin/hive -e "$HQL_vst"