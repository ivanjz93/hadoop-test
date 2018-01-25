package org.dan.mr.order_pro_mapjoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private Map<String, String> productMap = new HashMap<>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try(BufferedReader bin = new BufferedReader(new InputStreamReader(new FileInputStream("product.txt")))) {
			String line;
			while(StringUtils.isNotEmpty(line = bin.readLine())) {
				String[] productInfos = line.split(",");
				productMap.put(productInfos[0], productInfos[1]);
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] orderInfos = line.split(",");
		String pname = productMap.get(orderInfos[2]);
		context.write(new Text(line + "," + pname), NullWritable.get());
	}


}
