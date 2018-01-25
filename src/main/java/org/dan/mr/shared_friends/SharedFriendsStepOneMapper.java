package org.dan.mr.shared_friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SharedFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//A:B,C,D,F,E,O
		String line = value.toString();
		String[] fields = line.split(":");
		String person = fields[0];
		String[] friends = fields[1].split(",");
		for(String friend : friends) {
			context.write(new Text(friend), new Text(person));
		}
	}

}
