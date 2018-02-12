drop table if exists ods_weblog_origin;
create table ods_weblog_origin(
invalid string,
remote_addr string,
time_local string,
http_method string,
request string,
status string,
bytes string)
partitioned by (datestr string)
row format delimited
fields terminated by '\u0001';

drop table if exists ods_click_pageviews;
create table ods_click_pageviews(
Session string,
time_local string,
request string,
page_staylong string,
visit_step string)
partitioned by (datestr string)
row format delimited
fields terminated by '\u0001';

drop table if exists click_stream_visit;
create table click_stream_visit(
session string,
inTime string,
outTime string,
inPage string,
outPage string,
pageVisits int)
partitioned by (datestr string)
row format delimited
fields terminated by '\u0001';