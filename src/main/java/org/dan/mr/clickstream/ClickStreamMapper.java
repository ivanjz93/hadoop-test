package org.dan.mr.clickstream;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClickStreamMapper extends Mapper<LongWritable, Text, Text, PageView> {

	private Text outKey = new Text();
	private PageView outValue = new PageView();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PageView>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("\u0001");
		outKey.set(arr[0]);
		outValue.set(arr[0], arr[1], arr[2], arr[3], arr[4]);
		context.write(outKey, outValue);
	}

}
