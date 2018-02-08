package org.dan.mr.pageview;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.dan.mr.accesslog.AccessBean;

//false219.142.69.772018-02-08 14:32:06GET/userfiles/upload/man.png2001366
public class PageViewMapper extends Mapper<LongWritable, Text, Text, AccessBean> {

	private Text outKey = new Text();
	private AccessBean outValue = new AccessBean();
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, AccessBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split("\u0001");
		boolean invalid = Boolean.parseBoolean(arr[0]);
		if(!invalid) {
			outKey.set(arr[1]);
			outValue.set(invalid, arr[1], arr[2], arr[3], arr[4], arr[5], arr[6]);
			context.write(outKey, outValue);
		}
		
	}

	
}
