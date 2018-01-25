package org.dan.mr.wordindex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordIndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit)context.getInputSplit();
		String fileName = split.getPath().getName();
		String line = value.toString();
		String[] words = line.split(" ");
		for(String word : words) {
			context.write(new Text(word + "==" + fileName), new IntWritable(1));
		}
	}

}
