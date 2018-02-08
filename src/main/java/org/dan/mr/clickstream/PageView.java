package org.dan.mr.clickstream;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

//8289778f-6ddd-4298-b3b6-6d50d2724b1f2018-02-08 14:31:48/a/login;JSESSIONID=9e5c179d4f8a42cb844bc107d6d6090110002
public class PageView implements Writable{

	private String session;
	private String time;
	private String url;
	private String watchTime;
	private String step;
	
	public String getSession() {
		return session;
	}

	public void setSession(String session) {
		this.session = session;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getWatchTime() {
		return watchTime;
	}

	public void setWatchTime(String watchTime) {
		this.watchTime = watchTime;
	}

	public String getStep() {
		return step;
	}

	public void setStep(String step) {
		this.step = step;
	}

	public void set(String session, String time, String url, String watchTime, String step) {
		this.session = session;
		this.time = time;
		this.url = url;
		this.watchTime = watchTime;
		this.step = step;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		 session = input.readUTF();
		 time = input.readUTF();
		 url = input.readUTF();
		 watchTime = input.readUTF();
		 step = input.readUTF();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(session);
		output.writeUTF(time);
		output.writeUTF(url);
		output.writeUTF(watchTime);
		output.writeUTF(step);
	}

}
