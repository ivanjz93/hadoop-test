package org.dan.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] columns = line.split("\t");
		String number = columns[1];
		long upFlow = Long.parseLong(columns[columns.length - 3]);
		long downFlow = Long.parseLong(columns[columns.length - 2]);
		context.write(new Text(number), new FlowBean(upFlow, downFlow));
	}
	
}
