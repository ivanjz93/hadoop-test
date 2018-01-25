package org.dan.mr.flowsumsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSumDriver {
	private static String DEFAULT_INPUT_FILE_PATH = "/flowsum/output";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/flowsum/outputsort";
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(FlowSumDriver.class);
		
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		String fileInputPath = DEFAULT_INPUT_FILE_PATH;
		if(args.length > 0)
			fileInputPath = args[0];
		String fileOutputPath = DEFAULT_OUTPUT_FILE_PATH;
		if(args.length > 1)
			fileOutputPath = args[1];
		
		FileInputFormat.setInputPaths(job, new Path(fileInputPath));
		FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));
		
		job.waitForCompletion(true);
	}

}
