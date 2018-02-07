package org.dan.mr.accesslog;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccessLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	private Set<String> invalidPrefix;
	private Text outKey;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		invalidPrefix = new HashSet<String>();
		invalidPrefix.add("static");
		invalidPrefix.add("external");
		outKey = new Text();
	}
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		AccessBean accessBean = AccessBeanParser.parse(line);
		if(!accessBean.isInvalid() && invalidPrefix.contains(accessBean.getUrl().split("/")[1])) {
			accessBean.setInvalid(true);
		}
		outKey.set(accessBean.toString());
		context.write(outKey, NullWritable.get());
	}

	

}
