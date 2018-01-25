package org.dan.mr.shared_friends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SharedFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//A	I,K,C,B,G,F,H,O,D
		String line = value.toString();
		String[] fields = line.split("\t");
		String friend = fields[0];
		String[] persons = fields[1].split(",");
		Arrays.sort(persons);
		for(int i = 0; i < persons.length - 1; i++) {
			for(int j = i + 1; j < persons.length; j++) {
				context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
			}
		}
	}

}
