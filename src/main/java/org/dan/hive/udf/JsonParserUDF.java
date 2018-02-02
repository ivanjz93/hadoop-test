package org.dan.hive.udf;

import java.io.IOException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

public class JsonParserUDF extends UDF {

	private ObjectMapper mapper = new ObjectMapper();
	
	public String evaluate(String ratingLine) {
		try {
			return mapper.readValue(ratingLine, JsonRatingBean.class).toString();
		} catch (IOException e) {
			return "";
		}
	}
}
