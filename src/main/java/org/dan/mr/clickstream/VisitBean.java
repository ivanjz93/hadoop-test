package org.dan.mr.clickstream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VisitBean implements Writable{
	private String session;
	private String startTime;
	private String endTime;
	private String inUrl;
	private String outUrl;
	private int pageCounts;
	
	public String getSession() {
		return session;
	}
	public void setSession(String session) {
		this.session = session;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getEndTime() {
		return endTime;
	}
	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}
	public String getInUrl() {
		return inUrl;
	}
	public void setInUrl(String inUrl) {
		this.inUrl = inUrl;
	}
	public String getOutUrl() {
		return outUrl;
	}
	public void setOutUrl(String outUrl) {
		this.outUrl = outUrl;
	}
	public int getPageCounts() {
		return pageCounts;
	}
	public void setPageCounts(int pageCounts) {
		this.pageCounts = pageCounts;
	}
	
	public void set(String session, String startTime, String endTime, String inUrl, String outUrl, int pageCounts) {
		this.session = session;
		this.startTime = startTime;
		this.endTime = endTime;
		this.inUrl = inUrl;
		this.outUrl = outUrl;
		this.pageCounts = pageCounts;
	}
	
	@Override
	public String toString() {
		return session + "\u0001" + startTime + "\u0001" + endTime + "\u0001"
				+ inUrl + "\u0001" + outUrl + "\u0001" + pageCounts;
	}
	
	@Override
	public void readFields(DataInput input) throws IOException {
		session = input.readUTF();
		startTime = input.readUTF();
		endTime = input.readUTF();
		inUrl = input.readUTF();
		outUrl = input.readUTF();
		pageCounts = input.readInt();
	}
	
	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(session);
		output.writeUTF(startTime);
		output.writeUTF(endTime);
		output.writeUTF(inUrl);
		output.writeUTF(outUrl);
		output.writeInt(pageCounts);
	}
}
