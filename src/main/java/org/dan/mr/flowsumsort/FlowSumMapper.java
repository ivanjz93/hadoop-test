package org.dan.mr.flowsumsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

	private FlowBean flowBean = new FlowBean();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split("\t");
		String number = columns[0];
		long upFlow = Long.parseLong(columns[1]);
		long downFlow = Long.parseLong(columns[2]);
		flowBean.setFlow(upFlow, downFlow);
		context.write(flowBean, new Text(number));
	}
	
}
