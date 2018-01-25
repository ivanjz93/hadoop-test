package org.dan.mr.wordindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WordIndexStepTwo {

	private static String DEFAULT_INPUT_FILE_PATH = "/Users/ctbri/word_index/output";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/Users/ctbri/word_index/output_final";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(WordIndexStepTwo.class);
		
		job.setMapperClass(WordIndexStepTwoMapper.class);
		job.setReducerClass(WordIndexStepTwoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
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
