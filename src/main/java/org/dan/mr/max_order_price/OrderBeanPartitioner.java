package org.dan.mr.max_order_price;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderBeanPartitioner extends Partitioner<OrderBean, NullWritable> {

	@Override
	public int getPartition(OrderBean key, NullWritable value, int reduceTaskNum) {
		return (key.getOrderId().hashCode() & Integer.MAX_VALUE) % reduceTaskNum;
	}

}
