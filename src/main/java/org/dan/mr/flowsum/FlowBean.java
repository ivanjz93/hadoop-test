package org.dan.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{

	private long upFlow;
	private long downFlow;
	private long totalFlow;
	
	public FlowBean() {
		
	}

	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		totalFlow = upFlow + downFlow;
	}
	
	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getTotalFlow() {
		return totalFlow;
	}

	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		upFlow = input.readLong();
		downFlow = input.readLong();
		totalFlow = input.readLong();
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(upFlow);
		output.writeLong(downFlow);
		output.writeLong(totalFlow);
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + totalFlow;
	}
	
}
