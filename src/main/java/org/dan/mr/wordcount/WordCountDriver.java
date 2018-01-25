package org.dan.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordCountDriver {

	private static String DEFAULT_INPUT_FILE_PATH = "/wordcount/input";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/wordcount/output";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resourcemanager.hostname", "219.141.189.148");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WordCountDriver.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setCombinerClass(WordCountReducer.class);
		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMinInputSplitSize(job, 2097152);
		CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
		
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
