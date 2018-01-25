package org.dan.mr.max_order_price;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxOrderPriceReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}

}
