package org.dan.mr.accesslog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class AccessLogDriver {
	private static String DEFAULT_INPUT_FILE_PATH = "/flume/tomcat-access/18-02-07/1330";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/tomcat-access/output";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "219.141.189.148");
		conf.set("fs.defaultFS", "hdfs://mqM:9000");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(AccessLogDriver.class);
		
		job.setMapperClass(AccessLogMapper.class);
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
				
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
