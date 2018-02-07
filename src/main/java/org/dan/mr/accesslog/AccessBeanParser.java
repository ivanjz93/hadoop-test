package org.dan.mr.accesslog;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;

//219.141.189.148 - - [01/Feb/2018:07:59:08 +0800] "GET /zhhw/external/v1/trashcan?sn=863703032552288 HTTP/1.1" 200 244
public class AccessBeanParser {
	
	private static DateFormat logDF = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	private static DateFormat myDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static AccessBean parse(String line) {
		AccessBean bean = new AccessBean();
		String[] arr = line.split(" ");
		if(arr.length >= 10){
			bean.setIp(arr[0]);
			String time = formatTime(arr[3].substring(1));
			if(time == null)
				time = "-invalid_time-";
			bean.setTime(time);
			bean.setMethod(arr[5].substring(1));
			bean.setUrl(arr[6].substring(5));
			bean.setCode(arr[8]);
			bean.setBytes(arr[9]);
			
			if("-invalid_time-".equals(bean.getTime()))
				bean.setInvalid(true);
			if(Integer.parseInt(bean.getCode()) >= 400)
				bean.setInvalid(true);
		} else 
			bean.setInvalid(true);
		return bean;
	}
	
	private static String formatTime(String logTime) {
		try {
			return myDF.format(logDF.parse(logTime));
		} catch (Exception e){
			return null;
		}
	}
	public static void main(String[] args) throws Exception {
		System.out.println(myDF.format(logDF.parse("01/Feb/2018:07:59:08")));
	}
}
