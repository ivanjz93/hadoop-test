package org.dan.mr.wordindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordIndexStepOne {

	private static String DEFAULT_INPUT_FILE_PATH = "/Users/ctbri/word_index/input";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/Users/ctbri/word_index/output";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WordIndexStepOne.class);
		
		job.setMapperClass(WordIndexStepOneMapper.class);
		job.setReducerClass(WordIndexStepOneReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		String fileInputPath = DEFAULT_INPUT_FILE_PATH;
		if(args.length > 0)
			fileInputPath = args[0];
		String fileOutputPath = DEFAULT_OUTPUT_FILE_PATH;
		if(args.length > 1)
			fileOutputPath = args[1];
		FileInputFormat.setInputPaths(job, new Path(fileInputPath));
		FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));
		
		boolean result = job.waitForCompletion(true);
		System.out.println(result ? "run success" : "run fail");
	}

}
