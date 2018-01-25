package org.dan.mr.flowsum;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class NumPartitioner extends Partitioner<Text, FlowBean> {

	private static Map<String, Integer> locationMap = new HashMap<>();
	
	static {
		locationMap.put("136", 0);
		locationMap.put("137", 1);
		locationMap.put("138", 2);
		locationMap.put("139", 3);
	}
	@Override
	public int getPartition(Text key, FlowBean value, int numPatitions) {
		Integer locNum = locationMap.get(key.toString().substring(0, 3));
		return locNum == null ? 4 : locNum;
	}

}
