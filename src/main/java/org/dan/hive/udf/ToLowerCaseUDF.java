package org.dan.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class ToLowerCaseUDF extends UDF {

	public String evaluate(String value) {
		return value.toLowerCase();
	}
}
