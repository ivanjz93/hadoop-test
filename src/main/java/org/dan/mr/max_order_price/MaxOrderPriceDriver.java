package org.dan.mr.max_order_price;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class MaxOrderPriceDriver {

	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure();
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MaxOrderPriceDriver.class);
		
		job.setMapperClass(MaxOrderPriceMapper.class);
		job.setReducerClass(MaxOrderPriceReducer.class);
		
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setGroupingComparatorClass(OrderIdGroupingComparator.class);
		job.setPartitionerClass(OrderBeanPartitioner.class);
		
		FileInputFormat.setInputPaths(job, new Path("/Users/ctbri/price_max/input"));
		FileOutputFormat.setOutputPath(job, new Path("/Users/ctbri/price_max/output"));
		
		job.waitForCompletion(true);
	}

}
