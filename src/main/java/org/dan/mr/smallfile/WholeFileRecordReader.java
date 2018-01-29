package org.dan.mr.smallfile;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

	private FileSplit split;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.split = (FileSplit)split;
		conf = context.getConfiguration();
	}
	
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(!processed) {
			byte[] contents = new byte[(int)split.getLength()];
			Path file = split.getPath();
			FileSystem fs = FileSystem.get(conf);
			try(FSDataInputStream in= fs.open(file)) {
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			}
			processed = true;
			return true;
		}
		return false;
	}
	
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		return NullWritable.get();
	}
	
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1f :0f;
	}
	
	@Override
	public void close() throws IOException {
		
	}

}
