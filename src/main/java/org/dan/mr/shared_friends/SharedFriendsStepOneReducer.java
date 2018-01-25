package org.dan.mr.shared_friends;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SharedFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		for(Text value : values) {
			sb.append(value.toString());
			sb.append(",");
		}
		context.write(key, new Text(sb.substring(0, sb.length() - 1)));
	}

}
