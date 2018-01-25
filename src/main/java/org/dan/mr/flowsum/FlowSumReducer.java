package org.dan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		long upFlow = 0, downFlow = 0;
		for(FlowBean value : values) {
			upFlow += value.getUpFlow();
			downFlow += value.getDownFlow();
		}
		context.write(key, new FlowBean(upFlow, downFlow));
	}

}
