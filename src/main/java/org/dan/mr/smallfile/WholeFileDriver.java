package org.dan.mr.smallfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WholeFileDriver {

	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WholeFileDriver.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapperClass(SequenceFileMapper.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/ctbri/smallfile/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/ctbri/smallfile/output"));
		
		job.waitForCompletion(true);
		
	}

}
