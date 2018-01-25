package org.dan.mr.max_order_price;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxOrderPriceMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

	private OrderBean orderBean = new OrderBean();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//order_000003	pdt_01	222.8
		String line = value.toString();
		String[] fields = line.split("\t");
		orderBean.set(fields[0], fields[1], Double.parseDouble(fields[2]));
		context.write(orderBean, NullWritable.get());
	}

}
