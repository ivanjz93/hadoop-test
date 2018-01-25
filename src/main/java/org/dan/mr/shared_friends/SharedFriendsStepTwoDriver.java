package org.dan.mr.shared_friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class SharedFriendsStepTwoDriver {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SharedFriendsStepTwoDriver.class);
		
		job.setMapperClass(SharedFriendsStepTwoMapper.class);
		job.setReducerClass(SharedFriendsStepTwoReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/ctbri/shared_friends/middle"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/ctbri/shared_friends/output"));
		
		job.waitForCompletion(true);
	}

}
