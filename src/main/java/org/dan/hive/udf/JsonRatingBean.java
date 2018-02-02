package org.dan.hive.udf;
//{"movie":"1193", "rate":"5","timeStamp":"1517536323","uid":"3"}
public class JsonRatingBean {

	private String movie;
	private int rate;
	private long timeStamp;
	private String uid;
	public String getMovie() {
		return movie;
	}
	public void setMovie(String movie) {
		this.movie = movie;
	}
	public int getRate() {
		return rate;
	}
	public void setRate(int rate) {
		this.rate = rate;
	}
	public long getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getUid() {
		return uid;
	}
	public void setUid(String uid) {
		this.uid = uid;
	}
	@Override
	public String toString() {
		return movie + "\t" + rate + "\t" + timeStamp + "\t" + uid;
	}
	
}
