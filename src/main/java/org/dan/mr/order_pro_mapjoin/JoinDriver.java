package org.dan.mr.order_pro_mapjoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class JoinDriver {

	private static String DEFAULT_INPUT_FILE_PATH = "/Users/ctbri/order_pro/input";
	private static String DEFAULT_OUTPUT_FILE_PATH = "/Users/ctbri/order_pro/output";
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		//conf.set("mapreduce.framework.name", "yarn");
		//conf.set("yarn.resourcemanager.hostname", "219.141.189.148");
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JoinDriver.class);
		
		job.setMapperClass(JoinMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.addCacheFile(new URI("file:///Users/ctbri/order_pro/product.txt"));
		
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
