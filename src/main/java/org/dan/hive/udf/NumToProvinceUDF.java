package org.dan.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;

public class NumToProvinceUDF extends UDF {

	private static Map<String, String> numProMap = new HashMap<>();
	
	static {
		numProMap.put("136", "beijing");
		numProMap.put("137", "shanghai");
		numProMap.put("138", "guangzhou");
		numProMap.put("139", "shenzhen");
	}
	public String evaluate(String num) {
		String province = numProMap.get(num.substring(0, 3));
		if(province == null)
			province = "unknown";
		return province;
	}
}
