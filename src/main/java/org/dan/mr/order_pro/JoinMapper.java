package org.dan.mr.order_pro;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class JoinMapper extends Mapper<LongWritable, Text, Text, OrderProductBean> {
	
	private OrderProductBean bean = new OrderProductBean();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		FileSplit inputSplit = (FileSplit)context.getInputSplit();
		String name = inputSplit.getPath().getName();
		String line = value.toString();
		String[] fields = line.split(",");
		String pid;
		if("order.dat".equals(name)) {
			pid = fields[2];
			bean.set(fields[0], pid, fields[1], "", "", 0, Integer.parseInt(fields[3]), 0);
		} else {
			pid = fields[0];
			bean.set("", pid,  "", fields[1], fields[2], Double.parseDouble(fields[3]), 0, 1);
		}
		context.write(new Text(pid), bean);
	}


}
