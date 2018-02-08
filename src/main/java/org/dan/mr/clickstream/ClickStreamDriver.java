package org.dan.mr.clickstream;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;


public class ClickStreamDriver {
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	private static String DEFAULT_INPUT_FILE_PATH = "/znhw-pageview/" + df.format(new Date());
	private static String DEFAULT_OUTPUT_FILE_PATH = "/znhw-clickstream/" + df.format(new Date());
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.hostname", "219.141.189.148");
		conf.set("fs.defaultFS", "hdfs://mqM:9000");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(ClickStreamDriver.class);
		
		job.setMapperClass(ClickStreamMapper.class);
		job.setReducerClass(ClickStreamReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PageView.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(VisitBean.class);
				
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
