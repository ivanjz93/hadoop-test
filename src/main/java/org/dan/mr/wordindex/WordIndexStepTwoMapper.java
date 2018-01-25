package org.dan.mr.wordindex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordIndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] strs = line.split("==");
		if(strs.length >= 2)
			context.write(new Text(strs[0]), new Text(strs[1]));
	}

}
